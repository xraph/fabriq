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
