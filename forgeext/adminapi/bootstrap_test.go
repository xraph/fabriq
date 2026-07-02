package adminapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/fabriqtest"
)

// bootstrapKeyStore is a self-contained, in-memory KeyStore for the bootstrap
// and middleware-install tests. It persists Issue/List/Lookup/Revoke AND
// implements the concrete-store Ensure(ctx, key, spec) method that
// bootstrapAdminKey type-asserts for — so both the env-key path (Ensure) and
// the generated-key path (List + Issue) are exercised without a real DB.
//
// It lives in this file (not authn_middleware_test.go / keys_test.go) so it does
// not touch the off-limits fakes. The KeyStore interface deliberately has no
// Ensure method; Ensure is satisfied structurally, mirroring the concrete
// relationalKeyStore.
type bootstrapKeyStore struct {
	mu     sync.Mutex
	seq    int
	byHash map[string]*KeyRecord
	byID   map[string]*KeyRecord
}

func newBootstrapKeyStore() *bootstrapKeyStore {
	return &bootstrapKeyStore{byHash: map[string]*KeyRecord{}, byID: map[string]*KeyRecord{}}
}

// seed registers a pre-existing plaintext key without going through Issue.
func (s *bootstrapKeyStore) seed(plaintext string, rec KeyRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := rec
	s.byHash[hashKey(plaintext)] = &r
	s.byID[rec.ID] = &r
}

func (s *bootstrapKeyStore) Issue(_ context.Context, spec KeySpec) (IssuedKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("boot-%d", s.seq)
	key, prefix, hash, err := generateKey()
	if err != nil {
		return IssuedKey{}, err
	}
	rec := &KeyRecord{
		ID:            id,
		Prefix:        prefix,
		TenantID:      spec.TenantID,
		Label:         spec.Label,
		CanManageKeys: spec.CanManageKeys,
		CreatedAt:     time.Now().UTC(),
	}
	s.byHash[hash] = rec
	s.byID[id] = rec
	return IssuedKey{ID: id, Prefix: prefix, Key: key}, nil
}

func (s *bootstrapKeyStore) Lookup(_ context.Context, keyHash string) (KeyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byHash[keyHash]
	if !ok {
		return KeyRecord{}, false, nil
	}
	return *rec, true, nil
}

func (s *bootstrapKeyStore) List(context.Context) ([]KeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KeyRecord, 0, len(s.byID))
	for _, rec := range s.byID {
		out = append(out, *rec)
	}
	return out, nil
}

func (s *bootstrapKeyStore) Revoke(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[id]
	if !ok {
		return fmt.Errorf("bootstrapKeyStore: unknown key id %q", id)
	}
	now := time.Now().UTC()
	rec.RevokedAt = &now
	return nil
}

// Ensure mirrors relationalKeyStore.Ensure: INSERT ... ON CONFLICT DO NOTHING
// keyed on the hash. Returns existed=true when the hash was already present.
func (s *bootstrapKeyStore) Ensure(_ context.Context, key string, spec KeySpec) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := hashKey(key)
	if _, ok := s.byHash[h]; ok {
		return true, nil
	}
	s.seq++
	id := fmt.Sprintf("boot-ensure-%d", s.seq)
	rec := &KeyRecord{
		ID:            id,
		Prefix:        key[:min(7, len(key))],
		TenantID:      spec.TenantID,
		Label:         spec.Label,
		CanManageKeys: spec.CanManageKeys,
		CreatedAt:     time.Now().UTC(),
	}
	s.byHash[h] = rec
	s.byID[id] = rec
	return false, nil
}

var _ KeyStore = (*bootstrapKeyStore)(nil)

// countManageKeys returns how many non-revoked CanManageKeys keys are in the store.
func countManageKeys(t *testing.T, store KeyStore) int {
	t.Helper()
	recs, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	n := 0
	for _, r := range recs {
		if r.CanManageKeys && r.RevokedAt == nil {
			n++
		}
	}
	return n
}

// TestBootstrapAdminKey_EnvKey verifies that with FABRIQ_ADMIN_KEY set,
// bootstrap ensures a multi-tenant CanManageKeys row for exactly that key so it
// resolves via Lookup(hashKey(env)).
func TestBootstrapAdminKey_EnvKey(t *testing.T) {
	const envKey = "fq_env_admin_key"
	t.Setenv("FABRIQ_ADMIN_KEY", envKey)

	store := newBootstrapKeyStore()
	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	rec, found, err := store.Lookup(context.Background(), hashKey(envKey))
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found {
		t.Fatal("env key not present after bootstrap")
	}
	if !rec.CanManageKeys {
		t.Error("env key must have CanManageKeys")
	}
	if rec.TenantID != "" {
		t.Errorf("env key must be multi-tenant (TenantID == \"\"), got %q", rec.TenantID)
	}
}

// TestBootstrapAdminKey_EnvKey_Idempotent verifies running bootstrap twice with
// the same env key does not create a duplicate.
func TestBootstrapAdminKey_EnvKey_Idempotent(t *testing.T) {
	const envKey = "fq_env_admin_key"
	t.Setenv("FABRIQ_ADMIN_KEY", envKey)

	store := newBootstrapKeyStore()
	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap #1: %v", err)
	}
	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap #2: %v", err)
	}

	if got := countManageKeys(t, store); got != 1 {
		t.Fatalf("manage keys = %d, want 1 (idempotent env bootstrap)", got)
	}
}

// TestBootstrapAdminKey_Generated verifies that with no env key and an empty
// store, bootstrap issues exactly one CanManageKeys key, and running it again
// does not add a second (a manage key already exists).
func TestBootstrapAdminKey_Generated(t *testing.T) {
	t.Setenv("FABRIQ_ADMIN_KEY", "") // ensure unset for this test

	store := newBootstrapKeyStore()
	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap #1: %v", err)
	}
	if got := countManageKeys(t, store); got != 1 {
		t.Fatalf("after first bootstrap manage keys = %d, want 1", got)
	}

	// Idempotent: a manage key already exists → no second key issued.
	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap #2: %v", err)
	}
	if got := countManageKeys(t, store); got != 1 {
		t.Fatalf("after second bootstrap manage keys = %d, want 1 (idempotent)", got)
	}
}

// TestBootstrapAdminKey_Generated_ExistingManageKey verifies that when a
// CanManageKeys key already exists (e.g. seeded), bootstrap issues nothing.
func TestBootstrapAdminKey_Generated_ExistingManageKey(t *testing.T) {
	t.Setenv("FABRIQ_ADMIN_KEY", "")

	store := newBootstrapKeyStore()
	store.seed("fq_preexisting", KeyRecord{ID: "pre", TenantID: "", CanManageKeys: true})

	if err := bootstrapAdminKey(context.Background(), store); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if got := countManageKeys(t, store); got != 1 {
		t.Fatalf("manage keys = %d, want 1 (existing manage key must suppress issue)", got)
	}
}

// authInstallExt builds an Extension with WithAuth(store) but WITHOUT manually
// attaching authMiddleware — the whole point is that Routes() auto-installs the
// verify middleware because cfg.KeyStore != nil. The fabric is pre-resolved from
// a fresh test world, bypassing Start / fabriq.Open.
func authInstallExt(t *testing.T, store KeyStore) *Extension {
	t.Helper()
	world := buildTestWorld(t)
	e := NewAdminAPI(nil, WithAuth(store)) // no WithRouteOptions middleware
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	return e
}

// TestAuthInstall_MiddlewareGatesRealRoutes proves the middleware is auto-wired
// onto real controller routes when WithAuth is set: no Authorization → 401, a
// valid multi-tenant manage key + tenant selector → 200, and a non-manage key
// on /admin/keys → 403.
func TestAuthInstall_MiddlewareGatesRealRoutes(t *testing.T) {
	store := newBootstrapKeyStore()
	const adminKey = "fq_install_admin"
	const plainKey = "fq_install_plain"
	// Multi-tenant manage key (needs X-Tenant-ID selector).
	store.seed(adminKey, KeyRecord{ID: "admin", TenantID: "", CanManageKeys: true})
	// Tenant-bound non-manage key.
	store.seed(plainKey, KeyRecord{ID: "plain", TenantID: testTenantID, CanManageKeys: false})

	srv := buildServer(t, authInstallExt(t, store))
	defer srv.Close()

	// (1) No Authorization → 401 on a real route.
	noAuth := authReq(t, srv.URL, "/admin/meta", nil)
	noAuth.Body.Close()
	if noAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-auth /admin/meta status = %d, want 401", noAuth.StatusCode)
	}

	// (2) Valid manage key + tenant selector → 200.
	ok := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization": "Bearer " + adminKey,
		"X-Tenant-ID":   testTenantID,
	})
	defer ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(ok.Body)
		t.Fatalf("admin-key /admin/meta status = %d, want 200, body=%s", ok.StatusCode, b)
	}

	// (3) Non-manage key on /admin/keys → 403 (keys gate fires on a real route).
	forbidden := authReq(t, srv.URL, "/admin/keys", map[string]string{
		"Authorization": "Bearer " + plainKey,
		"X-Tenant-ID":   testTenantID,
	})
	defer forbidden.Body.Close()
	if forbidden.StatusCode != http.StatusForbidden {
		t.Fatalf("non-manage /admin/keys status = %d, want 403", forbidden.StatusCode)
	}
}

// TestWithAuth_SetsKeyStore is a focused unit check that WithAuth stores the
// KeyStore on the config (the field the install and registerKeyRoutes read).
func TestWithAuth_SetsKeyStore(t *testing.T) {
	store := newBootstrapKeyStore()
	var cfg config
	WithAuth(store)(&cfg)
	if cfg.KeyStore == nil {
		t.Fatal("WithAuth must set cfg.KeyStore")
	}
}
