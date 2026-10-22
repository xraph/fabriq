package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/fabriqtest"
)

// liveKeyStore is a self-contained, in-memory KeyStore that actually persists
// Issue/List/Revoke (unlike the Lookup-only fakeKeyStore in
// authn_middleware_test.go, which stubs writes to no-ops). It hashes issued
// keys the same way the middleware does, so a key it issues authenticates on
// subsequent requests, and Revoke stamps RevokedAt so authMiddleware denies it
// afterwards.
type liveKeyStore struct {
	mu     sync.Mutex
	seq    int
	byHash map[string]*KeyRecord
	byID   map[string]*KeyRecord
}

func newLiveKeyStore() *liveKeyStore {
	return &liveKeyStore{byHash: map[string]*KeyRecord{}, byID: map[string]*KeyRecord{}}
}

// seed registers a pre-existing plaintext key (e.g. a bootstrap admin key)
// without going through Issue, mirroring newFakeKeyStore.add.
func (s *liveKeyStore) seed(plaintext string, rec KeyRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := rec
	s.byHash[hashKey(plaintext)] = &r
	s.byID[rec.ID] = &r
}

func (s *liveKeyStore) Issue(_ context.Context, spec KeySpec) (IssuedKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("live-%d", s.seq)
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

// IssueSession is not exercised by the keys tests; it satisfies the KeyStore
// interface with a canned token.
func (s *liveKeyStore) IssueSession(_ context.Context, _ time.Duration) (IssuedKey, error) {
	return IssuedKey{}, fmt.Errorf("liveKeyStore: IssueSession not implemented")
}

func (s *liveKeyStore) Lookup(_ context.Context, keyHash string) (KeyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byHash[keyHash]
	if !ok {
		return KeyRecord{}, false, nil
	}
	return *rec, true, nil
}

func (s *liveKeyStore) List(context.Context) ([]KeyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KeyRecord, 0, len(s.byID))
	for _, rec := range s.byID {
		out = append(out, *rec)
	}
	return out, nil
}

func (s *liveKeyStore) Revoke(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[id]
	if !ok {
		return ErrKeyNotFound
	}
	// Mirror the relational store's COALESCE: a repeat revoke keeps the first
	// revokedAt.
	if rec.RevokedAt == nil {
		now := time.Now().UTC()
		rec.RevokedAt = &now
	}
	return nil
}

var _ KeyStore = (*liveKeyStore)(nil)

// keysReq issues a request to srv with the given method/path/body, stamping
// the Authorization bearer token when non-empty.
func keysReq(t *testing.T, method, srv, path, bearer string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// buildKeysExt builds an Extension backed by a fresh test world with auth
// enabled (WithAuth over an in-memory liveKeyStore) AND the auth middleware
// installed via WithRouteOptions, so the /admin/keys routes are registered
// (cfg.KeyStore != nil) and gated end-to-end — mirroring authTestExt in
// authn_middleware_test.go but also threading WithAuth so registerKeyRoutes
// fires. adminKey is pre-seeded into the store with CanManageKeys so callers
// can exercise the CRUD routes; it is tenant-bound to testTenantID.
func buildKeysExt(t *testing.T) (*Extension, string) {
	t.Helper()
	e, adminKey, _ := buildKeysExtWith(t)
	return e, adminKey
}

// buildKeysExtWith is buildKeysExt with extra options (for example a tenant
// registry), and it also returns the store so a test can seed records.
func buildKeysExtWith(t *testing.T, opts ...Option) (*Extension, string, *liveKeyStore) {
	t.Helper()
	world := buildTestWorld(t)
	store := newLiveKeyStore()
	const adminKey = "fq_admin_seed"
	store.seed(adminKey, KeyRecord{ID: "seed-admin", TenantID: testTenantID, CanManageKeys: true})

	all := append([]Option{
		WithAuth(store),
		WithRouteOptions(forge.WithMiddleware(authMiddleware(store, "/admin"))),
	}, opts...)
	e := NewAdminAPI(nil, all...)
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	return e, adminKey, store
}

func TestIssueKey_CreatesAndAuthenticates(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	body := strings.NewReader(`{"label":"ci-key","tenantId":"` + testTenantID + `"}`)
	resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201, body = %s", resp.StatusCode, b)
	}

	var got issueKeyResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID == "" {
		t.Error("id must not be empty")
	}
	if got.Prefix == "" {
		t.Error("prefix must not be empty")
	}
	if got.Key == "" {
		t.Error("key must not be empty")
	}
	if !strings.HasPrefix(got.Key, "fq_") {
		t.Errorf("key %q lacks fq_ prefix", got.Key)
	}

	// The returned key must itself authenticate against the admin surface.
	metaResp := keysReq(t, http.MethodGet, srv.URL, "/admin/meta", got.Key, nil)
	defer metaResp.Body.Close()
	if metaResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(metaResp.Body)
		t.Fatalf("issued key auth status = %d, want 200, body = %s", metaResp.StatusCode, b)
	}
}

func TestIssueKey_TenantBoundManageKey_StillWorks(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	// adminKey itself is tenant-bound (testTenantID) + CanManageKeys — confirm
	// it can issue successfully (the tenant-bound manage-key path).
	body := strings.NewReader(`{"label":"child-key"}`)
	resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201, body = %s", resp.StatusCode, b)
	}
}

func TestIssueKey_MissingLabel_400(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	body := strings.NewReader(`{}`)
	resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListKeys_ReturnsIssuedKeyWithoutSecret(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	issueBody := strings.NewReader(`{"label":"listed-key","tenantId":"` + testTenantID + `"}`)
	issueResp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, issueBody)
	defer issueResp.Body.Close()
	var issued issueKeyResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatalf("decode issue: %v", err)
	}

	listResp := keysReq(t, http.MethodGet, srv.URL, "/admin/keys", adminKey, nil)
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(listResp.Body)
		t.Fatalf("status = %d, want 200, body = %s", listResp.StatusCode, b)
	}

	rawBody, err := io.ReadAll(listResp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// No key/key_hash field anywhere in the raw JSON — redaction is structural,
	// but this guards against accidental field additions leaking a secret.
	if strings.Contains(string(rawBody), issued.Key) {
		t.Fatalf("list response leaks the plaintext key: %s", rawBody)
	}
	if strings.Contains(string(rawBody), `"key"`) || strings.Contains(string(rawBody), `"key_hash"`) || strings.Contains(string(rawBody), `"keyHash"`) {
		t.Fatalf("list response contains a key/hash field: %s", rawBody)
	}

	var got keyListResponse
	if err := json.Unmarshal(rawBody, &got); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	found := false
	for _, item := range got.Keys {
		if item.ID == issued.ID {
			found = true
			if item.Prefix != issued.Prefix {
				t.Errorf("prefix = %q, want %q", item.Prefix, issued.Prefix)
			}
			if item.Label != "listed-key" {
				t.Errorf("label = %q, want %q", item.Label, "listed-key")
			}
			if item.TenantID != testTenantID {
				t.Errorf("tenantId = %q, want %q", item.TenantID, testTenantID)
			}
			if item.RevokedAt != nil {
				t.Error("revokedAt must be nil for a fresh key")
			}
		}
	}
	if !found {
		t.Fatalf("issued key %q not present in list: %+v", issued.ID, got.Keys)
	}
}

func TestRevokeKey_RevokesAndBlocksSubsequentAuth(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	issueBody := strings.NewReader(`{"label":"revoke-me","tenantId":"` + testTenantID + `"}`)
	issueResp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, issueBody)
	defer issueResp.Body.Close()
	var issued issueKeyResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatalf("decode issue: %v", err)
	}

	// Sanity: the freshly issued key authenticates before revocation.
	preResp := keysReq(t, http.MethodGet, srv.URL, "/admin/meta", issued.Key, nil)
	preResp.Body.Close()
	if preResp.StatusCode != http.StatusOK {
		t.Fatalf("pre-revoke auth status = %d, want 200", preResp.StatusCode)
	}

	revokeResp := keysReq(t, http.MethodDelete, srv.URL, "/admin/keys/"+issued.ID, adminKey, nil)
	defer revokeResp.Body.Close()
	if revokeResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(revokeResp.Body)
		t.Fatalf("status = %d, want 200, body = %s", revokeResp.StatusCode, b)
	}
	var got revokeKeyResponse
	if err := json.NewDecoder(revokeResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode revoke: %v", err)
	}
	if !got.Revoked {
		t.Error("revoked must be true")
	}

	// Subsequent auth with the revoked key must 401.
	postResp := keysReq(t, http.MethodGet, srv.URL, "/admin/meta", issued.Key, nil)
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("post-revoke auth status = %d, want 401", postResp.StatusCode)
	}

	// GET reflects revokedAt set.
	listResp := keysReq(t, http.MethodGet, srv.URL, "/admin/keys", adminKey, nil)
	defer listResp.Body.Close()
	var list keyListResponse
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	found := false
	for _, item := range list.Keys {
		if item.ID == issued.ID {
			found = true
			if item.RevokedAt == nil {
				t.Error("revokedAt must be set after revoke")
			}
		}
	}
	if !found {
		t.Fatalf("revoked key %q not present in list", issued.ID)
	}
}

func TestKeyRoutes_NotRegistered_WhenNoAuth(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // no WithAuth
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/keys")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (route must not be registered without WithAuth)", resp.StatusCode)
	}
}

// readJSONError decodes a {"error": "..."} body.
func readJSONError(t *testing.T, resp *http.Response) string {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var got struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode error body %s: %v", raw, err)
	}
	return got.Error
}

func TestIssueKey_InvalidTenant_400(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	for _, bad := range []string{"bad tenant", "a:b", strings.Repeat("x", 65)} {
		body := strings.NewReader(`{"label":"k","tenantId":"` + bad + `"}`)
		resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, body)
		if resp.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("tenantId %q: status = %d, want 400, body = %s", bad, resp.StatusCode, b)
		}
		if got := readJSONError(t, resp); got != "invalid tenant id" {
			t.Errorf("tenantId %q: error = %q, want %q", bad, got, "invalid tenant id")
		}
		resp.Body.Close()
	}
}

func TestIssueKey_UnregisteredTenant_404(t *testing.T) {
	reg := newFakeTenantRegistry()
	if _, _, err := reg.Create(context.Background(), testTenantID, ""); err != nil {
		t.Fatal(err)
	}
	e, adminKey, store := buildKeysExtWith(t, WithTenantRegistry(reg))
	srv := buildServer(t, e)
	defer srv.Close()

	before := len(store.byID)
	body := strings.NewReader(`{"label":"k","tenantId":"acme"}`)
	resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404, body = %s", resp.StatusCode, b)
	}
	if got := readJSONError(t, resp); got != "unknown tenant" {
		t.Errorf("error = %q, want %q", got, "unknown tenant")
	}
	if after := len(store.byID); after != before {
		t.Errorf("store grew from %d to %d keys, want no key issued", before, after)
	}
}

func TestIssueKey_RegisteredTenant_201(t *testing.T) {
	reg := newFakeTenantRegistry()
	if _, _, err := reg.Create(context.Background(), "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	e, adminKey, _ := buildKeysExtWith(t, WithTenantRegistry(reg))
	srv := buildServer(t, e)
	defer srv.Close()

	for _, body := range []string{`{"label":"k","tenantId":"acme"}`, `{"label":"multi"}`} {
		resp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey, strings.NewReader(body))
		if resp.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("body %s: status = %d, want 201, body = %s", body, resp.StatusCode, b)
		}
		resp.Body.Close()
	}
}

func TestRevokeKey_Unknown_404(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := keysReq(t, http.MethodDelete, srv.URL, "/admin/keys/no-such-id", adminKey, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404, body = %s", resp.StatusCode, b)
	}
	if got := readJSONError(t, resp); got != "no such key" {
		t.Errorf("error = %q, want %q", got, "no such key")
	}
}

// listedRevokedAt returns the revokedAt the list route reports for id.
func listedRevokedAt(t *testing.T, srv, adminKey, id string) *string {
	t.Helper()
	resp := keysReq(t, http.MethodGet, srv, "/admin/keys", adminKey, nil)
	defer resp.Body.Close()
	var list keyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	for _, item := range list.Keys {
		if item.ID == id {
			return item.RevokedAt
		}
	}
	t.Fatalf("key %q not in list", id)
	return nil
}

func TestRevokeKey_Twice_200(t *testing.T) {
	e, adminKey := buildKeysExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	issueResp := keysReq(t, http.MethodPost, srv.URL, "/admin/keys", adminKey,
		strings.NewReader(`{"label":"twice","tenantId":"`+testTenantID+`"}`))
	var issued issueKeyResponse
	if err := json.NewDecoder(issueResp.Body).Decode(&issued); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	issueResp.Body.Close()

	var first *string
	for i := range 2 {
		resp := keysReq(t, http.MethodDelete, srv.URL, "/admin/keys/"+issued.ID, adminKey, nil)
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("revoke #%d: status = %d, want 200, body = %s", i+1, resp.StatusCode, b)
		}
		resp.Body.Close()

		got := listedRevokedAt(t, srv.URL, adminKey, issued.ID)
		if got == nil {
			t.Fatalf("revoke #%d: revokedAt is nil", i+1)
		}
		if first == nil {
			first = got
			time.Sleep(5 * time.Millisecond) // a moved timestamp would differ
			continue
		}
		if *got != *first {
			t.Errorf("revokedAt moved from %s to %s on a repeat revoke", *first, *got)
		}
	}
}

func TestListKeys_IncludesExpiresAt(t *testing.T) {
	e, adminKey, store := buildKeysExtWith(t)
	expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	store.seed("fq_session_seed", KeyRecord{ID: "session-1", Label: "session", CanManageKeys: true, ExpiresAt: &expires})
	srv := buildServer(t, e)
	defer srv.Close()

	resp := keysReq(t, http.MethodGet, srv.URL, "/admin/keys", adminKey, nil)
	defer resp.Body.Close()
	var list keyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	seen := 0
	for _, item := range list.Keys {
		switch item.ID {
		case "session-1":
			seen++
			if item.ExpiresAt == nil {
				t.Fatal("expiresAt missing for an expiring key")
			}
			if want := expires.Format(rfc3339Milli); *item.ExpiresAt != want {
				t.Errorf("expiresAt = %q, want %q", *item.ExpiresAt, want)
			}
		case "seed-admin":
			seen++
			if item.ExpiresAt != nil {
				t.Errorf("expiresAt = %q for a key that never expires, want omitted", *item.ExpiresAt)
			}
		}
	}
	if seen != 2 {
		t.Fatalf("saw %d of the 2 seeded keys in %+v", seen, list.Keys)
	}
}

func TestListKeys_IncludesLastUsedAt(t *testing.T) {
	e, adminKey, store := buildKeysExtWith(t)
	used := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	store.seed("fq_used_seed", KeyRecord{ID: "used-1", Label: "used", LastUsedAt: &used})
	srv := buildServer(t, e)
	defer srv.Close()

	resp := keysReq(t, http.MethodGet, srv.URL, "/admin/keys", adminKey, nil)
	defer resp.Body.Close()
	var list keyListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	seen := 0
	for _, item := range list.Keys {
		switch item.ID {
		case "used-1":
			seen++
			if item.LastUsedAt == nil {
				t.Fatal("lastUsedAt missing for a key that has been used")
			}
			if want := used.Format(rfc3339Milli); *item.LastUsedAt != want {
				t.Errorf("lastUsedAt = %q, want %q", *item.LastUsedAt, want)
			}
		case "seed-admin":
			seen++
			if item.LastUsedAt != nil {
				t.Errorf("lastUsedAt = %q for a never-used key, want omitted", *item.LastUsedAt)
			}
		}
	}
	if seen != 2 {
		t.Fatalf("saw %d of the 2 seeded keys in %+v", seen, list.Keys)
	}
}

// TestKeyRoutes_SlashVariants_NonManageKey_403 is C1 end to end on the real
// controller routes: a non-manage key can neither list nor mint keys by
// doubling a slash.
func TestKeyRoutes_SlashVariants_NonManageKey_403(t *testing.T) {
	e, _, store := buildKeysExtWith(t)
	const plainKey = "fq_plain_seed"
	store.seed(plainKey, KeyRecord{ID: "seed-plain", TenantID: testTenantID})
	srv := buildServer(t, e)
	defer srv.Close()

	before := len(store.byID)
	for _, path := range []string{"/admin//keys", "//admin/keys", "/admin/keys/"} {
		resp := keysReq(t, http.MethodPost, srv.URL, path, plainKey,
			strings.NewReader(`{"label":"escalate","canManageKeys":true}`))
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("POST %s: status = %d, want 403, body = %s", path, resp.StatusCode, b)
		}

		resp = keysReq(t, http.MethodGet, srv.URL, path, plainKey, nil)
		b, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s: status = %d, want 403, body = %s", path, resp.StatusCode, b)
		}
	}
	resp := keysReq(t, http.MethodDelete, srv.URL, "/admin//keys/seed-admin", plainKey, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("DELETE /admin//keys/seed-admin: status = %d, want 403", resp.StatusCode)
	}
	if after := len(store.byID); after != before {
		t.Fatalf("store grew from %d to %d keys, want none minted", before, after)
	}
}
