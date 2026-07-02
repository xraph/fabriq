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
		return fmt.Errorf("liveKeyStore: unknown key id %q", id)
	}
	now := time.Now().UTC()
	rec.RevokedAt = &now
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
	world := buildTestWorld(t)
	store := newLiveKeyStore()
	const adminKey = "fq_admin_seed"
	store.seed(adminKey, KeyRecord{ID: "seed-admin", TenantID: testTenantID, CanManageKeys: true})

	e := NewAdminAPI(nil,
		WithAuth(store),
		WithRouteOptions(forge.WithMiddleware(authMiddleware(store, "/admin"))),
	)
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	return e, adminKey
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
