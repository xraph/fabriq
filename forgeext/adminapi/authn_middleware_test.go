package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/tenant"
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

// Revoke reports ErrKeyNotFound for an id no stored record carries, matching
// the relational store. It does not mutate anything: the middleware tests
// never revoke.
func (f *fakeKeyStore) Revoke(_ context.Context, id string) error {
	for _, rec := range f.byHash {
		if rec.ID == id {
			return nil
		}
	}
	return ErrKeyNotFound
}

// authTestExt builds an Extension backed by a fresh test world with the auth
// middleware (over the fake store) wired via WithRouteOptions. Unlike the shared
// fakeBackedAdminExt (which installs the header-only tenantMiddleware), this
// installs authMiddleware so the full auth path is exercised. The fabric is
// pre-resolved from the world, bypassing Start / fabriq.Open.
func authTestExt(t *testing.T, store KeyStore) *Extension {
	t.Helper()
	world := buildTestWorld(t)
	e := NewAdminAPI(nil, // nil parent — bypass forgeext.Extension
		WithRouteOptions(forge.WithMiddleware(authMiddleware(store, "/admin"))),
	)
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

// buildInstanceProbeServer registers a GET probe at each path, guarded by the
// auth middleware. The handler echoes the stashed key id and whether a tenant
// was stamped, so the instance-route behaviour can be asserted from outside.
func buildInstanceProbeServer(t *testing.T, store KeyStore, paths ...string) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "auth-instance-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error {
		id, _ := resolvedKeyID(ctx.Request().Context())
		tid, terr := tenant.FromContext(ctx.Request().Context())
		return ctx.JSON(http.StatusOK, map[string]any{"keyID": id, "tenant": tid, "hasTenant": terr == nil})
	}
	for _, p := range paths {
		if err := app.Router().GET(p, handler,
			forge.WithMiddleware(authMiddleware(store, "/admin"))); err != nil {
			t.Fatalf("register probe route %s: %v", p, err)
		}
	}
	return httptest.NewServer(app.Router().Handler())
}

// instanceProbe is the decoded body of buildInstanceProbeServer's handler.
type instanceProbe struct {
	KeyID     string `json:"keyID"`
	Tenant    string `json:"tenant"`
	HasTenant bool   `json:"hasTenant"`
}

func decodeInstanceProbe(t *testing.T, resp *http.Response) instanceProbe {
	t.Helper()
	var got instanceProbe
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode probe: %v", err)
	}
	return got
}

func TestAuthMiddleware_MultiTenantManageKey_NoSelector_KeysRoute_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k-multi-admin", CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, "/admin/keys", "/admin/keys/:id")
	defer srv.Close()

	for _, path := range []string{"/admin/keys", "/admin/keys/abc"} {
		resp := authReq(t, srv.URL, path, map[string]string{"Authorization": "Bearer fq_multi_admin"})
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("%s: status = %d, want 200, body = %s", path, resp.StatusCode, body)
		}
		assertVersionHeader(t, resp)
		got := decodeInstanceProbe(t, resp)
		resp.Body.Close()
		if got.HasTenant {
			t.Errorf("%s: tenant %q stamped, want none on an instance route", path, got.Tenant)
		}
		if got.KeyID != "k-multi-admin" {
			t.Errorf("%s: keyID = %q, want k-multi-admin", path, got.KeyID)
		}
	}
}

func TestAuthMiddleware_MultiTenantManageKey_NoSelector_TenantsRoute_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k-multi-admin", CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, "/admin/tenants", "/admin/tenants/:id")
	defer srv.Close()

	for _, path := range []string{"/admin/tenants", "/admin/tenants/acme"} {
		resp := authReq(t, srv.URL, path, map[string]string{"Authorization": "Bearer fq_multi_admin"})
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("%s: status = %d, want 200, body = %s", path, resp.StatusCode, body)
		}
		got := decodeInstanceProbe(t, resp)
		resp.Body.Close()
		if got.HasTenant {
			t.Errorf("%s: tenant %q stamped, want none on an instance route", path, got.Tenant)
		}
		if got.KeyID != "k-multi-admin" {
			t.Errorf("%s: keyID = %q, want k-multi-admin", path, got.KeyID)
		}
	}
}

func TestAuthMiddleware_MultiTenantKey_NoSelector_EntitiesRoute_400(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k1", CanManageKeys: true})
	srv := buildServer(t, authTestExt(t, store))
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/entities?type=product", map[string]string{"Authorization": "Bearer fq_multi_admin"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400 (data routes still need X-Tenant-ID), body = %s", resp.StatusCode, body)
	}
	assertVersionHeader(t, resp)
}

func TestAuthMiddleware_TenantBoundKey_TenantsRoute_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	srv := buildInstanceProbeServer(t, store, "/admin/tenants", "/admin/tenants/:id")
	defer srv.Close()

	for _, path := range []string{"/admin/tenants", "/admin/tenants/" + testTenantID} {
		resp := authReq(t, srv.URL, path, map[string]string{"Authorization": "Bearer fq_bound"})
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: status = %d, want 403, body = %s", path, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "this API key may not manage tenants") {
			t.Errorf("%s: body = %s, want the manage-tenants message", path, body)
		}
	}
}

func TestAuthMiddleware_KeyspaceSibling_NotInstanceRoute(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k1", CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, "/admin/keyspace", "/admin/tenantsx")
	defer srv.Close()

	for _, path := range []string{"/admin/keyspace", "/admin/tenantsx"} {
		resp := authReq(t, srv.URL, path, map[string]string{"Authorization": "Bearer fq_multi_admin"})
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400 (a sibling is not an instance route)", path, resp.StatusCode)
		}
	}
}

// rawReq sends method + path to srv, keeping path byte for byte (no client-side
// cleaning), with an optional bearer token.
func rawReq(t *testing.T, method, srv, path, bearer string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv+path, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", path, err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request %s: %v", path, err)
	}
	return resp
}

// instanceProbePaths are the route patterns the path-form tests register, so
// the router (forgemux) and the middleware both see every request.
var instanceProbePaths = []string{
	"/admin/keys", "/admin/keys/:id", "/admin/tenants", "/admin/tenants/:id", "/admin/keyspace",
}

// TestAuthMiddleware_SlashVariants_NonManageKey_403 pins C1: forgemux drops
// empty segments when it routes, so every one of these paths reaches a keys or
// tenants handler, and the gate must see it as one.
func TestAuthMiddleware_SlashVariants_NonManageKey_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_plain", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	srv := buildInstanceProbeServer(t, store, instanceProbePaths...)
	defer srv.Close()

	for _, path := range []string{
		"/admin//keys", "//admin/keys", "/admin/keys//x", "/admin/keys/", "/admin%2Fkeys",
		"/admin//tenants", "//admin//tenants/acme", "/admin/tenants/", "/admin/tenants%2Facme",
	} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_plain")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403, body = %s", path, resp.StatusCode, body)
		}
	}
}

// TestAuthMiddleware_SlashVariants_ManageKey_InstanceRoute_200 checks the other
// direction: the instance-route relaxation follows the same path form.
func TestAuthMiddleware_SlashVariants_ManageKey_InstanceRoute_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k-multi-admin", CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, instanceProbePaths...)
	defer srv.Close()

	for _, path := range []string{"/admin//keys", "//admin/keys/", "//admin//tenants/acme"} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_multi_admin")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status = %d, want 200, body = %s", path, resp.StatusCode, body)
		}
	}
}

// TestAuthMiddleware_SlashVariants_KeyspaceSibling_400 confirms a sibling with
// extra slashes is still a data route.
func TestAuthMiddleware_SlashVariants_KeyspaceSibling_400(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k1", CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, instanceProbePaths...)
	defer srv.Close()

	for _, path := range []string{"/admin//keyspace", "//admin/keyspace/"} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_multi_admin")
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (a sibling is not an instance route)", path, resp.StatusCode)
		}
	}
}

// TestAuthMiddleware_RouterDoesNotRoute_CaseOrDots pins the two ways a path
// could differ that the router itself refuses: static segments match case
// sensitively and dot segments are literal. These never reach a keys handler,
// so the middleware's case-sensitive, dot-preserving form agrees with it.
func TestAuthMiddleware_RouterDoesNotRoute_CaseOrDots(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_plain", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	srv := buildInstanceProbeServer(t, store, "/admin/keys", "/admin/tenants")
	defer srv.Close()

	for _, path := range []string{"/admin/KEYS", "/Admin/keys", "/admin/./keys", "/admin/x/../keys", "/admin/Tenants"} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_plain")
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404 (the router does not route it)", path, resp.StatusCode)
		}
	}
}

// TestAuthMiddleware_LoginExemption_SlashVariant keeps the login exemption on
// the routed path too: "//admin/login" reaches the login handler, so it is
// exempt like "/admin/login".
func TestAuthMiddleware_LoginExemption_SlashVariant(t *testing.T) {
	srv := buildLoginProbeServer(t, newFakeKeyStore())
	defer srv.Close()

	resp := rawReq(t, http.MethodPost, srv.URL, "//admin//login", "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAuthMiddleware_TenantBoundManageKey_TenantsRoute_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_bound_admin", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: true})
	srv := buildInstanceProbeServer(t, store, instanceProbePaths...)
	defer srv.Close()

	for _, path := range []string{"/admin/tenants", "/admin/tenants/" + testTenantID, "//admin//tenants"} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_bound_admin")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: status = %d, want 403, body = %s", path, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "this API key may not manage tenants") {
			t.Errorf("%s: body = %s, want the manage-tenants message", path, body)
		}
	}

	// R11: the same key keeps its /keys access.
	resp := rawReq(t, http.MethodGet, srv.URL, "/admin/keys", "fq_bound_admin")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/admin/keys: status = %d, want 200 for a tenant-bound manage key", resp.StatusCode)
	}
}

func TestRoutePath(t *testing.T) {
	for in, want := range map[string]string{
		"":                  "/",
		"/":                 "/",
		"//":                "/",
		"/admin":            "/admin",
		"/admin/":           "/admin",
		"admin":             "/admin",
		"//admin//keys//x/": "/admin/keys/x",
		"/admin/./keys":     "/admin/./keys",
		"/admin/../keys":    "/admin/../keys",
		"/Admin/Keys":       "/Admin/Keys",
	} {
		if got := routePath(in); got != want {
			t.Errorf("routePath(%q) = %q, want %q", in, got, want)
		}
	}
}

// touchCall is one recorded KeyToucher.Touch invocation.
type touchCall struct {
	id string
	at time.Time
}

// touchingKeyStore is a fakeKeyStore that also implements KeyToucher, so the
// middleware's optional-capability assertion finds it. Touch records the call
// and returns err (nil by default).
type touchingKeyStore struct {
	*fakeKeyStore
	mu    sync.Mutex
	calls []touchCall
	err   error
}

func newTouchingKeyStore(err error) *touchingKeyStore {
	return &touchingKeyStore{fakeKeyStore: newFakeKeyStore(), err: err}
}

func (s *touchingKeyStore) Touch(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, touchCall{id: id, at: at})
	return s.err
}

func (s *touchingKeyStore) recorded() []touchCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]touchCall(nil), s.calls...)
}

// testClock is a settable clock for the touch throttle.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// buildTouchProbeServer serves GET /admin/meta-probe behind authMiddleware
// with a throttle on the injected clock whose touches run synchronously, so a
// test can assert the recorded calls the moment the response returns.
func buildTouchProbeServer(t *testing.T, store *touchingKeyStore, clock *testClock) *httptest.Server {
	t.Helper()
	th := newTouchThrottle(store)
	th.now = clock.now
	th.spawn = func(fn func()) { fn() }
	app := forge.NewApp(forge.AppConfig{Name: "auth-touch-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error { return ctx.JSON(http.StatusOK, map[string]any{"ok": true}) }
	if err := app.Router().GET("/admin/keys", handler,
		forge.WithMiddleware(authMiddlewareWithTouch(store, "/admin", th))); err != nil {
		t.Fatalf("register touch probe route: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func TestAuthMiddleware_TouchesKeyOncePerMinute(t *testing.T) {
	store := newTouchingKeyStore(nil)
	store.add("fq_admin", KeyRecord{ID: "k1", CanManageKeys: true})
	store.add("fq_other", KeyRecord{ID: "k2", CanManageKeys: true})
	t0 := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clock := &testClock{t: t0}
	srv := buildTouchProbeServer(t, store, clock)
	defer srv.Close()

	hit := func(key string) {
		t.Helper()
		resp := authReq(t, srv.URL, "/admin/keys", map[string]string{"Authorization": "Bearer " + key})
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
	}

	hit("fq_admin") // t0: touched
	clock.set(t0.Add(10 * time.Second))
	hit("fq_admin") // inside the minute: skipped
	clock.set(t0.Add(11 * time.Second))
	hit("fq_other") // another key has its own window: touched
	clock.set(t0.Add(70 * time.Second))
	hit("fq_admin") // past the minute: touched

	got := store.recorded()
	want := []touchCall{
		{id: "k1", at: t0},
		{id: "k2", at: t0.Add(11 * time.Second)},
		{id: "k1", at: t0.Add(70 * time.Second)},
	}
	if len(got) != len(want) {
		t.Fatalf("Touch calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Touch call %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAuthMiddleware_DeniedRequest_DoesNotTouch(t *testing.T) {
	store := newTouchingKeyStore(nil)
	store.add("fq_plain", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	clock := &testClock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	srv := buildTouchProbeServer(t, store, clock)
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/keys", map[string]string{"Authorization": "Bearer fq_plain"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if got := store.recorded(); len(got) != 0 {
		t.Fatalf("Touch calls = %+v, want none for a denied request", got)
	}
}

func TestAuthMiddleware_TouchFailureDoesNotFailRequest(t *testing.T) {
	store := newTouchingKeyStore(errors.New("db down"))
	store.add("fq_admin", KeyRecord{ID: "k1", CanManageKeys: true})
	clock := &testClock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	srv := buildTouchProbeServer(t, store, clock)
	defer srv.Close()

	resp := authReq(t, srv.URL, "/admin/keys", map[string]string{"Authorization": "Bearer fq_admin"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 even though Touch fails", resp.StatusCode)
	}
	if got := store.recorded(); len(got) != 1 {
		t.Fatalf("Touch calls = %+v, want the one failing attempt", got)
	}
}

// TestTouchThrottle_PrunesExpiredEntries checks the throttle map does not
// keep an entry for every id it ever saw: entries older than the interval go
// on the next insert.
func TestTouchThrottle_PrunesExpiredEntries(t *testing.T) {
	store := newTouchingKeyStore(nil)
	t0 := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clock := &testClock{t: t0}
	th := newTouchThrottle(store)
	th.now = clock.now
	th.spawn = func(fn func()) { fn() }

	for _, id := range []string{"a", "b", "c"} {
		th.touch(id)
	}
	size := func() int {
		th.mu.Lock()
		defer th.mu.Unlock()
		return len(th.last)
	}
	if got := size(); got != 3 {
		t.Fatalf("map size = %d, want 3 within the interval", got)
	}

	clock.set(t0.Add(touchInterval - time.Second))
	th.touch("d")
	if got := size(); got != 4 {
		t.Fatalf("map size = %d, want 4 (nothing is old enough to prune yet)", got)
	}

	clock.set(t0.Add(touchInterval + time.Second))
	th.touch("e")
	if got := size(); got != 2 {
		t.Fatalf("map size = %d, want 2 (d and e) after the first three expired", got)
	}
	// An expired id is touched again, not throttled.
	th.touch("a")
	if got := len(store.recorded()); got != 6 {
		t.Fatalf("Touch calls = %d, want 6 (a, b, c, d, e, then a again)", got)
	}
}

// TestTouchThrottle_RunsDetachedWithTimeout drives the real goroutine path:
// the touch context must not be the request's, and must carry a deadline.
func TestTouchThrottle_RunsDetachedWithTimeout(t *testing.T) {
	type seen struct {
		hasDeadline bool
		remaining   time.Duration
	}
	done := make(chan seen, 1)
	th := newTouchThrottle(touchFunc(func(ctx context.Context, _ string, _ time.Time) error {
		dl, ok := ctx.Deadline()
		done <- seen{hasDeadline: ok, remaining: time.Until(dl)}
		return nil
	}))
	th.touch("k1")
	select {
	case got := <-done:
		if !got.hasDeadline || got.remaining > touchTimeout {
			t.Fatalf("touch ctx deadline = %+v, want one within %s", got, touchTimeout)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("touch never ran")
	}
}

// touchFunc adapts a function to the KeyStore + KeyToucher pair the throttle
// asserts on. Only Touch is used.
type touchFunc func(ctx context.Context, id string, at time.Time) error

func (f touchFunc) Touch(ctx context.Context, id string, at time.Time) error { return f(ctx, id, at) }
func (touchFunc) Issue(context.Context, KeySpec) (IssuedKey, error)          { return IssuedKey{}, nil }
func (touchFunc) IssueSession(context.Context, time.Duration) (IssuedKey, error) {
	return IssuedKey{}, nil
}
func (touchFunc) Lookup(context.Context, string) (KeyRecord, bool, error) {
	return KeyRecord{}, false, nil
}
func (touchFunc) List(context.Context) ([]KeyRecord, error) { return nil, nil }
func (touchFunc) Revoke(context.Context, string) error      { return nil }

// buildMigrationsProbeServer registers probes for the migration routes behind
// the auth middleware: GET status, GET a job, and the POST up route.
func buildMigrationsProbeServer(t *testing.T, store KeyStore) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "auth-migrations-probe", HTTPAddress: ":0"})
	handler := func(ctx forge.Context) error {
		_, terr := tenant.FromContext(ctx.Request().Context())
		return ctx.JSON(http.StatusOK, map[string]any{"hasTenant": terr == nil})
	}
	mw := forge.WithMiddleware(authMiddleware(store, "/admin"))
	if err := app.Router().GET("/admin/migrations", handler, mw); err != nil {
		t.Fatalf("register GET migrations: %v", err)
	}
	if err := app.Router().GET("/admin/migrations/jobs/:id", handler, mw); err != nil {
		t.Fatalf("register GET migrations job: %v", err)
	}
	if err := app.Router().POST("/admin/migrations/up", handler, mw); err != nil {
		t.Fatalf("register POST migrations up: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

func TestAuthMiddleware_MigrationsStatus_ManageKeyNoSelector_200(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k-multi-admin", CanManageKeys: true})
	srv := buildMigrationsProbeServer(t, store)
	defer srv.Close()

	for _, path := range []string{"/admin/migrations", "/admin/migrations/", "//admin//migrations"} {
		resp := rawReq(t, http.MethodGet, srv.URL, path, "fq_multi_admin")
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200, body = %s", path, resp.StatusCode, body)
		}
		if strings.Contains(string(body), `"hasTenant":true`) {
			t.Errorf("GET %s: tenant stamped, want none on an instance route", path)
		}
	}
}

func TestAuthMiddleware_MigrationsStatus_NonManageKey_403(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_plain", KeyRecord{ID: "k1", TenantID: testTenantID, CanManageKeys: false})
	store.add("fq_plain_multi", KeyRecord{ID: "k2", CanManageKeys: false})
	srv := buildMigrationsProbeServer(t, store)
	defer srv.Close()

	for _, key := range []string{"fq_plain", "fq_plain_multi"} {
		for _, path := range []string{"/admin/migrations", "/admin//migrations", "//admin/migrations/", "/admin%2Fmigrations"} {
			resp := rawReq(t, http.MethodGet, srv.URL, path, key)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("%s GET %s: status = %d, want 403, body = %s", key, path, resp.StatusCode, body)
			}
		}
	}
}

func TestAuthMiddleware_MigrationsUp_StillDataRoute(t *testing.T) {
	store := newFakeKeyStore()
	store.add("fq_multi_admin", KeyRecord{ID: "k-multi-admin", CanManageKeys: true})
	srv := buildMigrationsProbeServer(t, store)
	defer srv.Close()

	// The POST route and the sub-path GET are data routes: a multi-tenant key
	// with no selector still gets the 400.
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/admin/migrations/up"},
		{http.MethodGet, "/admin/migrations/jobs/abc"},
	} {
		resp := rawReq(t, tc.method, srv.URL, tc.path, "fq_multi_admin")
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %s: status = %d, want 400", tc.method, tc.path, resp.StatusCode)
		}
	}

	// With a selector the same route is reachable.
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/migrations/up", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer fq_multi_admin")
	req.Header.Set("X-Tenant-ID", testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /admin/migrations/up with selector: status = %d, want 200", resp.StatusCode)
	}
}
