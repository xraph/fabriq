package adminapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/fabriqtest"
)

// fakeKeyStore is an in-memory KeyStore for middleware tests. It resolves keys
// by their sha256 hex hash (the same value the middleware derives via hashKey),
// with NO tenant scoping and NO DB. Only Lookup is exercised by the middleware;
// the write methods satisfy the interface and are unused.
type fakeKeyStore struct {
	byHash map[string]KeyRecord
}

func newFakeKeyStore() *fakeKeyStore {
	return &fakeKeyStore{byHash: map[string]KeyRecord{}}
}

// add registers a plaintext key mapped to a KeyRecord, hashing it the same way
// the middleware does so Lookup(hashKey(key)) resolves it.
func (f *fakeKeyStore) add(plaintext string, rec KeyRecord) {
	f.byHash[hashKey(plaintext)] = rec
}

func (f *fakeKeyStore) Issue(context.Context, KeySpec) (IssuedKey, error) {
	return IssuedKey{}, nil
}

// IssueSession is not exercised by the middleware tests (which only drive
// Lookup); it satisfies the KeyStore interface with a canned token.
func (f *fakeKeyStore) IssueSession(context.Context, time.Duration) (IssuedKey, error) {
	return IssuedKey{}, nil
}

func (f *fakeKeyStore) Lookup(_ context.Context, keyHash string) (KeyRecord, bool, error) {
	rec, ok := f.byHash[keyHash]
	return rec, ok, nil
}

func (f *fakeKeyStore) List(context.Context) ([]KeyRecord, error) { return nil, nil }

func (f *fakeKeyStore) Revoke(context.Context, string) error { return nil }

// authTestExt builds an Extension backed by a fresh test world with the auth
// middleware (over the fake store) wired via WithRouteOptions. Unlike the shared
// fakeBackedAdminExt (which installs the header-only tenantMiddleware), this
// installs authMiddleware so the full auth path is exercised. The fabric is
// pre-resolved from the world, bypassing Start / fabriq.Open.
func authTestExt(t *testing.T, store KeyStore, opts ...Option) *Extension {
	t.Helper()
	world := buildTestWorld(t)
	opts = append([]Option{
		WithRouteOptions(forge.WithMiddleware(authMiddleware(store, "/admin"))),
	}, opts...)
	e := NewAdminAPI(nil, opts...) // nil parent — bypass forgeext.Extension
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	return e
}

// authReq issues a request to srv with the supplied headers and returns the
// response. Headers with an empty value are skipped.
func authReq(t *testing.T, srv string, path string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// assertVersionHeader fails if the always-on X-Fabriq-Api-Version response
// header is missing or wrong.
func assertVersionHeader(t *testing.T, resp *http.Response) {
	t.Helper()
	got := resp.Header.Get(apiVersionHeader)
	if got == "" {
		t.Errorf("%s header absent, want %q", apiVersionHeader, apiVersionValue())
	} else if got != apiVersionValue() {
		t.Errorf("%s = %q, want %q", apiVersionHeader, got, apiVersionValue())
	}
}

// decodeTenant reads the /admin/meta response and returns the echoed tenant.
func decodeTenant(t *testing.T, resp *http.Response) string {
	t.Helper()
	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("decode meta: %v (body=%s)", err, body)
	}
	return got.Tenant
}

func TestAuthMiddleware_MissingAuthorization_401(t *testing.T) {
	store := newFakeKeyStore()
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_MalformedAuthorization_401(t *testing.T) {
	store := newFakeKeyStore()
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	// No "Bearer " prefix.
	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Token abc"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_UnknownKey_401(t *testing.T) {
	store := newFakeKeyStore() // empty
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Bearer fq_nope"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_RevokedKey_401(t *testing.T) {
	store := newFakeKeyStore()
	now := time.Now().UTC()
	store.add("fq_revoked", KeyRecord{ID: "k1", TenantID: testTenantID, RevokedAt: &now})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Bearer fq_revoked"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_ExpiredKey_401(t *testing.T) {
	store := newFakeKeyStore()
	past := time.Now().UTC().Add(-1 * time.Hour)
	store.add("fq_expired", KeyRecord{ID: "k1", TenantID: testTenantID, ExpiresAt: &past})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Bearer fq_expired"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_TenantBoundKey_ResolvesTenant_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	// Case-insensitive "bearer" prefix must also work.
	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "bearer fq_bound"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
	if got := decodeTenant(t, resp); got != testTenantID {
		t.Errorf("tenant = %q, want %q", got, testTenantID)
	}
}

func TestAuthMiddleware_TenantBoundKey_MatchingSelector_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	// X-Tenant-ID present and equal to the bound tenant → allowed.
	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization": "Bearer fq_bound",
		"X-Tenant-ID":   testTenantID,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
	if got := decodeTenant(t, resp); got != testTenantID {
		t.Errorf("tenant = %q, want %q", got, testTenantID)
	}
}

func TestAuthMiddleware_TenantBoundKey_MismatchSelector_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization": "Bearer fq_bound",
		"X-Tenant-ID":   "some-other-tenant",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_MultiTenantKey_NoSelector_400(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi", KeyRecord{ID: "k1", TenantID: ""}) // multi-tenant
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Bearer fq_multi"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_MultiTenantKey_WithSelector_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi", KeyRecord{ID: "k1", TenantID: ""})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization": "Bearer fq_multi",
		"X-Tenant-ID":   testTenantID,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
	if got := decodeTenant(t, resp); got != testTenantID {
		t.Errorf("tenant = %q, want %q", got, testTenantID)
	}
}

// buildKeysProbeServer registers a trivial GET /admin/keys route guarded by the
// auth middleware, so the keys-gate branch (which keys on the request PATH) can
// be exercised. The shared admin controller registers no /admin/keys route, and
// route-scoped middleware only fires on a matched route — so without this probe
// the request would 404 at routing before the middleware runs.
func buildKeysProbeServer(t *testing.T, store KeyStore) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "auth-keys-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error { return ctx.JSON(http.StatusOK, map[string]any{"ok": true}) }
	if err := app.Router().GET("/admin/keys", handler,
		forge.WithMiddleware(authMiddleware(store, "/admin"))); err != nil {
		t.Fatalf("register keys probe route: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func TestAuthMiddleware_KeysRoute_NonManageKey_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_plain", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	srv := buildKeysProbeServer(t, store)
	defer srv.Close()

	// Any path under /admin/keys must be forbidden without CanManageKeys.
	resp := authReq(t, srv.URL, "/admin/keys", map[string]string{"Authorization": "Bearer fq_plain"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_KeysRoute_ManageKey_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_admin", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: true})
	srv := buildKeysProbeServer(t, store)
	defer srv.Close()

	// A key with CanManageKeys passes the keys gate.
	resp := authReq(t, srv.URL, "/admin/keys", map[string]string{"Authorization": "Bearer fq_admin"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_VersionMajorMismatch_426(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization":        "Bearer fq_bound",
		"X-Fabriq-Api-Version": "999",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("status = %d, want 426", resp.StatusCode)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_VersionMatch_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{
		"Authorization":        "Bearer fq_bound",
		"X-Fabriq-Api-Version": apiVersionValue(),
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
}

// buildLoginProbeServer registers a trivial POST /admin/login route guarded by
// the auth middleware, so the top-of-body exemption (which keys on method +
// exact path) can be exercised without a real login handler.
func buildLoginProbeServer(t *testing.T, store KeyStore) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "auth-login-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error { return ctx.JSON(http.StatusOK, map[string]any{"ok": true}) }
	if err := app.Router().POST("/admin/login", handler,
		forge.WithMiddleware(authMiddleware(store, "/admin"))); err != nil {
		t.Fatalf("register login probe route: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func TestAuthMiddleware_LoginRoute_NoAuthorization_200(t *testing.T) {
	store := newFakeKeyStore() // empty — no key could possibly resolve
	srv := buildLoginProbeServer(t, store)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/login", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	// Deliberately no Authorization header: /login is the way in.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (login must be reachable with no auth), body = %s", resp.StatusCode, body)
	}
}

// buildKeyIDProbeServer registers a GET /admin/whoami route guarded by the
// auth middleware whose handler echoes resolvedKeyID(ctx) back as JSON, so the
// stash can be asserted from outside the package.
func buildKeyIDProbeServer(t *testing.T, store KeyStore) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "auth-keyid-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error {
		id, ok := resolvedKeyID(ctx.Request().Context())
		return ctx.JSON(http.StatusOK, map[string]any{"keyID": id, "ok": ok})
	}
	if err := app.Router().GET("/admin/whoami", handler,
		forge.WithMiddleware(authMiddleware(store, "/admin"))); err != nil {
		t.Fatalf("register whoami probe route: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func TestAuthMiddleware_ValidKey_StashesResolvedKeyID(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k-stashed-1", TenantID: testTenantID})
	srv := buildKeyIDProbeServer(t, store)
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/whoami", map[string]string{"Authorization": "Bearer fq_bound"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	var got struct {
		KeyID string `json:"keyID"`
		OK    bool   `json:"ok"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode whoami: %v", err)
	}
	if !got.OK {
		t.Fatal("resolvedKeyID: ok = false, want true")
	}
	if got.KeyID != "k-stashed-1" {
		t.Errorf("resolvedKeyID = %q, want %q", got.KeyID, "k-stashed-1")
	}
}

func TestAuthMiddleware_VersionAbsent_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	// No X-Fabriq-Api-Version header at all → tolerated.
	resp := authReq(t, srv.URL, "/admin/meta", map[string]string{"Authorization": "Bearer fq_bound"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
}
