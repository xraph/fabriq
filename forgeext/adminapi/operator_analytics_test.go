package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/fabriqtest"
)

const analyticsOperatorAuth = "Bearer fq_test_analytics_operator"

func operatorAnalyticsStore() *fakeKeyStore {
	store := newFakeKeyStore()
	store.add("fq_test_analytics_operator", KeyRecord{ID: "operator", CanManageKeys: true})
	return store
}

type analyticsLookupErrorStore struct{ *fakeKeyStore }

func (s analyticsLookupErrorStore) Lookup(ctx context.Context, hash string) (KeyRecord, bool, error) {
	rec, found, err := s.fakeKeyStore.Lookup(ctx, hash)
	if hash == hashKey("fq_test_analytics_lookup_error") {
		return KeyRecord{ID: "lookup-error", CanManageKeys: true}, true, errors.New("test lookup failed")
	}
	return rec, found, err
}

func operatorAnalyticsRequest(t *testing.T, srv *httptest.Server, method, path, authorization, selector string) *http.Response {
	t.Helper()
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader(`{"sql":"SELECT 1"}`)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", authorization)
	req.Header.Set(testTenantHeader, selector)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Fabriq-Role", "operator")
	req.Header.Set("X-User-ID", "operator")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func assertOperatorAnalyticsMeta(t *testing.T, srv *httptest.Server, authorization string, read, admin bool) {
	t.Helper()
	resp := operatorAnalyticsRequest(t, srv, http.MethodGet, "/admin/meta", authorization, testTenantID)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("meta status = %d, want 200", resp.StatusCode)
	}
	var meta metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	if got := slices.Contains(meta.Capabilities, "analytics.read"); got != read {
		t.Errorf("meta analytics.read = %v, want %v", got, read)
	}
	if got := slices.Contains(meta.Capabilities, "analytics.admin"); got != admin {
		t.Errorf("meta analytics.admin = %v, want %v", got, admin)
	}
	if !slices.Contains(meta.Capabilities, "entities.read") {
		t.Error("meta omits entities.read")
	}
}

func TestOperatorOnlyAnalyticsIdentity(t *testing.T) {
	store := operatorAnalyticsStore()
	past := time.Now().Add(-time.Hour)
	store.add("fq_test_analytics_revoked", KeyRecord{ID: "revoked", CanManageKeys: true, RevokedAt: &past})
	store.add("fq_test_analytics_expired", KeyRecord{ID: "expired", CanManageKeys: true, ExpiresAt: &past})
	store.add("fq_test_analytics_tenant", KeyRecord{ID: "tenant", TenantID: testTenantID})
	store.add("fq_test_analytics_tenant_manage", KeyRecord{ID: "tenant-manage", TenantID: testTenantID, CanManageKeys: true})
	store.add("fq_test_analytics_unrestricted", KeyRecord{ID: "unrestricted"})
	store.add("fq_bootstrap_lookalike", KeyRecord{Label: "bootstrap", Prefix: "fq_bootstrap"})

	var analyticsDelegations atomic.Int32
	e := NewAdminAPI(nil, WithAuth(analyticsLookupErrorStore{store}), WithOperatorOnlyAnalytics(),
		WithAuthorizer(AuthorizerFunc(func(_ context.Context, capability string) (bool, error) {
			switch capability {
			case "analytics.read", "analytics.admin":
				analyticsDelegations.Add(1)
			}
			return true, nil
		})))
	srv := buildServer(t, e)
	defer srv.Close()

	for _, tc := range []struct {
		name, authorization, selector string
		want                          int
	}{
		{"missing", "", "", http.StatusUnauthorized},
		{"malformed", "Token fq_test_analytics_operator", "", http.StatusUnauthorized},
		{"empty bearer", "Bearer", "", http.StatusUnauthorized},
		{"unknown", "Bearer fq_test_analytics_unknown", "", http.StatusUnauthorized},
		{"revoked", "Bearer fq_test_analytics_revoked", "", http.StatusUnauthorized},
		{"expired", "Bearer fq_test_analytics_expired", "", http.StatusUnauthorized},
		{"lookup error", "Bearer fq_test_analytics_lookup_error", "", http.StatusInternalServerError},
		{"tenant", "Bearer fq_test_analytics_tenant", "", http.StatusForbidden},
		{"tenant with selector", "Bearer fq_test_analytics_tenant", testTenantID, http.StatusForbidden},
		{"tenant manager", "Bearer fq_test_analytics_tenant_manage", "", http.StatusForbidden},
		{"tenant manager with selector", "Bearer fq_test_analytics_tenant_manage", testTenantID, http.StatusForbidden},
		{"tenant manager with mismatched selector", "Bearer fq_test_analytics_tenant_manage", "other-tenant", http.StatusForbidden},
		{"unrestricted non-manager", "Bearer fq_test_analytics_unrestricted", "", http.StatusForbidden},
		{"unrestricted non-manager with selector", "Bearer fq_test_analytics_unrestricted", testTenantID, http.StatusForbidden},
		{"bootstrap lookalike without id", "Bearer fq_bootstrap_lookalike", testTenantID, http.StatusForbidden},
		{"operator with invalid selector", analyticsOperatorAuth, "invalid tenant", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, route := range []struct{ method, path string }{
				{http.MethodGet, "/admin/analytics/status"},
				{http.MethodPost, "/admin/analytics/query"},
			} {
				resp := operatorAnalyticsRequest(t, srv, route.method, route.path, tc.authorization, tc.selector)
				resp.Body.Close()
				if resp.StatusCode != tc.want {
					t.Errorf("%s %s: status = %d, want %d", route.method, route.path, resp.StatusCode, tc.want)
				}
			}
		})
	}

	for _, authorization := range []string{
		"Bearer fq_test_analytics_tenant",
		"Bearer fq_test_analytics_tenant_manage",
		"Bearer fq_test_analytics_unrestricted",
		"Bearer fq_bootstrap_lookalike",
	} {
		assertOperatorAnalyticsMeta(t, srv, authorization, false, false)
	}
	if got := analyticsDelegations.Load(); got != 0 {
		t.Errorf("analytics authorizer called %d times for refused requests", got)
	}
}

func TestOperatorOnlyAnalyticsCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name, selector string
		opts           []Option
		read, admin    bool
	}{
		{"default flags", testTenantID, nil, false, false},
		{"operator option only", "", []Option{WithOperatorOnlyAnalytics()}, false, false},
		{"read only", "", []Option{WithOperatorOnlyAnalytics(), WithAnalyticsRead()}, true, false},
		{"admin implies read", "", []Option{WithOperatorOnlyAnalytics(), WithAnalyticsAdmin()}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := operatorAnalyticsStore()
			store.add("fq_test_analytics_tenant_manage", KeyRecord{ID: "tenant-manage", TenantID: testTenantID, CanManageKeys: true})
			e := NewAdminAPI(nil, append([]Option{WithAuth(store)}, tc.opts...)...)
			controller := newAdminController(e)
			if tc.admin {
				controller.analyticsJobs.jobs["job-1"] = &analyticsJob{ID: "job-1", State: "done"}
			} else {
				// An early job lookup would dereference the nil registry.
				controller.analyticsJobs = nil
			}
			app := forge.NewApp(forge.AppConfig{Name: "operator-analytics-test", HTTPAddress: ":0"})
			if err := app.RegisterController(controller); err != nil {
				t.Fatalf("register controller: %v", err)
			}
			srv := httptest.NewServer(app.Router().Handler())
			defer srv.Close()

			assertOperatorAnalyticsMeta(t, srv, analyticsOperatorAuth, tc.read, tc.admin)
			assertOperatorAnalyticsMeta(t, srv, "Bearer fq_test_analytics_tenant_manage", false, false)

			for _, route := range []struct {
				method, path string
				allowed      bool
				want         int
			}{
				{http.MethodGet, "/status", tc.read, http.StatusOK},
				{http.MethodPost, "/query", tc.read, http.StatusNotImplemented},
				{http.MethodPost, "/backfill", tc.admin, http.StatusBadRequest},
				{http.MethodPost, "/reconcile", tc.admin, http.StatusBadRequest},
				{http.MethodPost, "/reproject", tc.admin, http.StatusBadRequest},
				{http.MethodPost, "/purge", tc.admin, http.StatusBadRequest},
				{http.MethodGet, "/jobs/job-1", tc.admin, http.StatusOK},
				{http.MethodGet, "/jobs/job-1/stream", tc.admin, http.StatusOK},
			} {
				t.Run(route.path, func(t *testing.T) {
					path := "/admin/analytics" + route.path
					resp := operatorAnalyticsRequest(t, srv, route.method, path, analyticsOperatorAuth, tc.selector)
					defer resp.Body.Close()
					want := http.StatusForbidden
					if route.allowed {
						want = route.want
					}
					if resp.StatusCode != want {
						t.Fatalf("status = %d, want %d", resp.StatusCode, want)
					}
					if got, wantSSE := resp.Header.Get("Content-Type") == "text/event-stream", tc.admin && strings.HasSuffix(route.path, "/stream"); got != wantSSE {
						t.Errorf("stream opened = %v, want %v", got, wantSSE)
					}

					tenantResp := operatorAnalyticsRequest(t, srv, route.method, path, "Bearer fq_test_analytics_tenant_manage", testTenantID)
					defer tenantResp.Body.Close()
					if tenantResp.StatusCode != http.StatusForbidden {
						t.Errorf("tenant manager status = %d, want 403", tenantResp.StatusCode)
					}
				})
			}
		})
	}
}

func TestOperatorOnlyAnalyticsCustomAuthorizer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		auth     Option
		allowed  bool
		err      error
		wantRead int
		wantJob  int
	}{
		{"allow", WithAuth(operatorAnalyticsStore()), true, nil, http.StatusOK, http.StatusNotFound},
		{"deny", WithAuth(operatorAnalyticsStore()), false, nil, http.StatusForbidden, http.StatusForbidden},
		{"error wins over allow", WithAuth(operatorAnalyticsStore()), true, errors.New("test authorization failed"), http.StatusInternalServerError, http.StatusInternalServerError},
		{"auth disabled", WithAuthDisabled(), true, nil, http.StatusForbidden, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewAdminAPI(nil, tc.auth, WithOperatorOnlyAnalytics(), WithAnalyticsAdmin(),
				WithAuthorizer(AuthorizerFunc(func(_ context.Context, capability string) (bool, error) {
					switch capability {
					case "analytics.read", "analytics.admin":
						return tc.allowed, tc.err
					default:
						return true, nil
					}
				})))
			srv := buildServer(t, e)
			defer srv.Close()
			for _, route := range []struct {
				path string
				want int
			}{
				{"/admin/analytics/status", tc.wantRead},
				{"/admin/analytics/jobs/missing", tc.wantJob},
				{"/admin/analytics/jobs/missing/stream", tc.wantJob},
			} {
				resp := operatorAnalyticsRequest(t, srv, http.MethodGet, route.path, analyticsOperatorAuth, "")
				resp.Body.Close()
				if resp.StatusCode != route.want {
					t.Errorf("%s: status = %d, want %d", route.path, resp.StatusCode, route.want)
				}
			}
			assertOperatorAnalyticsMeta(t, srv, analyticsOperatorAuth, tc.wantRead == http.StatusOK, tc.wantJob == http.StatusNotFound)
		})
	}
}

func TestOperatorOnlyAnalyticsRouteShapes(t *testing.T) {
	const base = "/custom/admin"
	store := operatorAnalyticsStore()
	world := buildTestWorld(t)
	e := NewAdminAPI(nil, WithAuth(store), WithOperatorOnlyAnalytics(), WithAnalyticsRead(), WithBasePath(base))
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	app := forge.NewApp(forge.AppConfig{Name: "operator-analytics-routes-test", HTTPAddress: ":0"})
	if err := app.RegisterController(newAdminController(e)); err != nil {
		t.Fatalf("register controller: %v", err)
	}
	router := app.Router()

	// Register probe routes with the same auth middleware. This lets a request
	// exercise tenant selection for paths and methods the router would otherwise reject.
	probe := func(ctx forge.Context) error { return ctx.JSON(http.StatusOK, nil) }
	getPaths := []string{
		"/analytics", "/analytics/query", "/analytics/jobs", "/analytics/jobs/job-1/other",
		"/analytics/jobs/job-1/stream/more", "/analytics/status/more", "/Analytics/status",
		"/analytics-other/status", "/analytics/../status",
	}
	for _, path := range getPaths {
		if err := router.GET(base+path, probe, e.cfg.RouteOptions...); err != nil {
			t.Fatalf("register GET %s: %v", path, err)
		}
	}
	postPaths := []string{"/analytics/status", "/analytics/jobs/job-1", "/analytics/query/more"}
	for _, path := range postPaths {
		if err := router.POST(base+path, probe, e.cfg.RouteOptions...); err != nil {
			t.Fatalf("register POST %s: %v", path, err)
		}
	}
	srv := httptest.NewServer(router.Handler())
	defer srv.Close()

	assertStatus := func(method, path, selector string, want int) {
		t.Helper()
		resp := operatorAnalyticsRequest(t, srv, method, path, analyticsOperatorAuth, selector)
		defer resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s %s: status = %d, want %d", method, path, resp.StatusCode, want)
		}
	}
	assertStatus(http.MethodGet, base+"/analytics/status", "", http.StatusOK)
	assertStatus(http.MethodGet, "/custom//admin//analytics%2Fstatus/", "", http.StatusOK)
	assertStatus(http.MethodPost, "/custom//admin//analytics/query/", "", http.StatusNotImplemented)
	assertStatus(http.MethodPost, base+"/analytics/query", testTenantID, http.StatusNotImplemented)
	assertStatus(http.MethodGet, base+"/meta", "", http.StatusBadRequest)
	assertStatus(http.MethodGet, base+"/entities?type=widget", "", http.StatusBadRequest)
	assertStatus(http.MethodGet, base+"/entities?type=widget", testTenantID, http.StatusOK)
	for _, path := range getPaths {
		assertStatus(http.MethodGet, base+path, "", http.StatusBadRequest)
	}
	for _, path := range postPaths {
		assertStatus(http.MethodPost, base+path, "", http.StatusBadRequest)
	}
}

func TestOperatorOnlyAnalyticsOffPreservesEmbedding(t *testing.T) {
	store := operatorAnalyticsStore()
	store.add("fq_test_analytics_tenant", KeyRecord{ID: "tenant", TenantID: testTenantID})
	e := NewAdminAPI(nil, WithAuth(store), WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()

	tenantResp := operatorAnalyticsRequest(t, srv, http.MethodGet, "/admin/analytics/status", "Bearer fq_test_analytics_tenant", "")
	defer tenantResp.Body.Close()
	if tenantResp.StatusCode != http.StatusOK {
		t.Errorf("tenant read with option off = %d, want 200", tenantResp.StatusCode)
	}

	operatorResp := operatorAnalyticsRequest(t, srv, http.MethodGet, "/admin/analytics/status", analyticsOperatorAuth, "")
	defer operatorResp.Body.Close()
	if operatorResp.StatusCode != http.StatusBadRequest {
		t.Errorf("selector-free operator with option off = %d, want 400", operatorResp.StatusCode)
	}
	assertOperatorAnalyticsMeta(t, srv, "Bearer fq_test_analytics_tenant", true, false)
}
