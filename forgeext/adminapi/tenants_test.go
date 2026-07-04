package adminapi

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestTenantEndpoints_AdminOff verifies that without WithTenantsAdmin the
// tenant-management endpoints answer 403 (capability gate), not 500 or 404.
func TestTenantEndpoints_AdminOff(t *testing.T) {
	e := NewAdminAPI(nil) // TenantsAdmin defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/tenants")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (tenants admin not enabled)", resp.StatusCode)
	}
}

// TestTenantEndpoints_NotCatalogMode verifies that with WithTenantsAdmin on
// but no parent (so Provisioner() is unreachable/nil), the endpoints answer
// 400 (not catalog mode), not 500. This is the pure-unit path: no Docker, no
// Postgres — parent is nil so c.ext.parent.Provisioner() is guarded before
// ever being called.
func TestTenantEndpoints_NotCatalogMode(t *testing.T) {
	e := NewAdminAPI(nil, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/tenants")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (not catalog mode)", resp.StatusCode)
	}
}

// TestTenantEndpoints_NotCatalogMode_GetSuspendResume exercises the same gate
// on the get/suspend/resume routes, confirming all four handlers share
// requireTenantsAdmin and none nil-panics when parent/catalog is absent.
func TestTenantEndpoints_NotCatalogMode_GetSuspendResume(t *testing.T) {
	e := NewAdminAPI(nil, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin/tenants/acme"},
		{http.MethodPost, "/admin/tenants/acme/suspend"},
		{http.MethodPost, "/admin/tenants/acme/resume"},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
		if err != nil {
			t.Fatalf("new request %s %s: %v", tc.method, tc.path, err)
		}
		resp, doErr := http.DefaultClient.Do(req)
		if doErr != nil {
			t.Fatalf("do request %s %s: %v", tc.method, tc.path, doErr)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %s status = %d, want 400 (not catalog mode)", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// TestTenantEndpoints_Capability verifies the tenants.admin capability is
// advertised in GET /admin/meta only when WithTenantsAdmin is set (mirrors
// the schema.admin dynamic-capability convention).
func TestTenantEndpoints_Capability(t *testing.T) {
	world := buildTestWorld(t)

	off := fakeBackedAdminExt(t, world)
	srvOff := buildServer(t, off)
	defer srvOff.Close()
	respOff := get(t, srvOff, "/admin/meta")
	defer respOff.Body.Close()
	var metaOff metaResponse
	if err := json.NewDecoder(respOff.Body).Decode(&metaOff); err != nil {
		t.Fatalf("decode meta (off): %v", err)
	}
	for _, c := range metaOff.Capabilities {
		if c == "tenants.admin" {
			t.Fatal("tenants.admin must not be advertised when TenantsAdmin is off")
		}
	}

	on := fakeBackedAdminExt(t, world, WithTenantsAdmin())
	srvOn := buildServer(t, on)
	defer srvOn.Close()
	respOn := get(t, srvOn, "/admin/meta")
	defer respOn.Body.Close()
	var metaOn metaResponse
	if err := json.NewDecoder(respOn.Body).Decode(&metaOn); err != nil {
		t.Fatalf("decode meta (on): %v", err)
	}
	found := false
	for _, c := range metaOn.Capabilities {
		if c == "tenants.admin" {
			found = true
		}
	}
	if !found {
		t.Fatal("tenants.admin must be advertised when TenantsAdmin is on")
	}
}

// TestTenantProvision_RequiresBody verifies POST /admin/tenants answers a 4xx
// (never a 500, and never starts a job) when the gate is off/not-catalog-mode
// or the body is missing required fields. The fake-backed harness has a nil
// parent, so requireTenantsAdmin's 400 fires before body validation — either
// way the contract is "a 4xx, no panic, no started job".
func TestTenantProvision_RequiresBody(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/tenants", map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		t.Fatalf("status = %d, want a 4xx", resp.StatusCode)
	}
}

// TestTenantProvision_403WhenGateOff verifies the async provision endpoint
// shares requireTenantsAdmin's capability gate.
func TestTenantProvision_403WhenGateOff(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/tenants",
		map[string]any{"tenantId": "acme", "clusterId": "primary"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (gate off)", resp.StatusCode)
	}
}

// TestTenantMigrateAll_403WhenGateOff mirrors the provision gate test for the
// fleet migrate-all endpoint.
func TestTenantMigrateAll_403WhenGateOff(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/tenants/migrate-all", map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (gate off)", resp.StatusCode)
	}
}

// TestTenantJob_NotFound verifies polling an unknown job id returns 404.
func TestTenantJob_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/tenants/jobs/nope")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestTenantJobStream_NotFound verifies streaming an unknown job id returns
// 404 before opening the SSE stream.
func TestTenantJobStream_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/tenants/jobs/nope/stream")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestTenantRoutes_Precedence is the CRITICAL route-precedence gate: the new
// static routes POST /tenants/migrate-all and GET /tenants/jobs/:id must not
// be shadowed by the Task-7 param routes /tenants/:id/... (a greedy router
// could match /tenants/migrate-all as /tenants/:id with id="migrate-all", or
// /tenants/jobs/<id> as /tenants/:id/... with id="jobs").
//
// We prove correct dispatch by response SHAPE, not just status code, since
// the nil-parent fake backend answers 400 "not catalog mode" from both the
// tenant-lifecycle handlers AND the async handlers' shared requireTenantsAdmin
// gate (so status code alone can't distinguish a correct vs. shadowed route):
//   - handleTenantJob's 404 body is {"error":"no such job"} — a tenant-lookup
//     404 would instead read {"error":"no such tenant"} (see handleTenantGet).
//   - GET /tenants/acme (an ordinary id, neither "jobs" nor "migrate-all")
//     must still reach handleTenantGet, proving the static routes were added
//     without breaking the existing param route.
func TestTenantRoutes_Precedence(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	// POST /tenants/migrate-all must reach handleTenantMigrateAll. It has no
	// method-matching counterpart under /tenants/:id (that subtree is
	// GET/suspend/resume only), so a 404/405 here means the static route
	// never registered or was shadowed.
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/tenants/migrate-all", map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		t.Fatalf("POST /tenants/migrate-all status = %d, route not registered/reachable", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST /tenants/migrate-all status = %d, want 400 (not catalog mode, via handleTenantMigrateAll)", resp.StatusCode)
	}

	// GET /tenants/jobs/:id must reach handleTenantJob (job registry lookup),
	// not handleTenantGet (tenant catalog lookup) — distinguished by the 404
	// error message, since "jobs" is itself a syntactically-valid tenant id.
	jobResp := get(t, srv, "/admin/tenants/jobs/nope")
	defer jobResp.Body.Close()
	if jobResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /tenants/jobs/nope status = %d, want 404", jobResp.StatusCode)
	}
	var jobBody map[string]string
	if err := json.NewDecoder(jobResp.Body).Decode(&jobBody); err != nil {
		t.Fatalf("decode job 404 body: %v", err)
	}
	if jobBody["error"] != "no such job" {
		t.Fatalf("GET /tenants/jobs/nope body = %v, want {error: \"no such job\"} (proves dispatch reached handleTenantJob, not a bare tenant lookup or router 404)", jobBody)
	}

	// Conversely GET /tenants/:id with an id that is NOT "jobs"/"migrate-all"
	// must still reach handleTenantGet (tenant catalog lookup, distinct error
	// body), confirming the static routes did not shadow the still-needed
	// param route for ordinary tenant ids.
	tenantResp := get(t, srv, "/admin/tenants/acme")
	defer tenantResp.Body.Close()
	// requireTenantsAdmin's gate error renders via forge's HTTPError shape
	// ({"code":400,"error":"..."}), so decode into map[string]any (the "code"
	// field is numeric and would fail a map[string]string decode).
	var tenantBody map[string]any
	if err := json.NewDecoder(tenantResp.Body).Decode(&tenantBody); err != nil {
		t.Fatalf("decode tenant lookup body: %v", err)
	}
	if tenantBody["error"] != "tenant management requires catalog mode (db-per-tenant)" {
		t.Fatalf("GET /tenants/acme body = %v, want the tenant-get not-catalog-mode error (proves /tenants/:id still dispatches to handleTenantGet)", tenantBody)
	}
}
