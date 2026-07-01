package adminapi

import (
	"net/http"
	"testing"
)

// The projections status endpoint reads Postgres-backed bookkeeping via the
// StateRepo seam, which the fake-backed harness (nil parent → nil stateRepo)
// does not provide, so it reports 501. The populated happy path is covered by
// the live admin-demo verification.
func TestProjections_NotAvailableOnFake(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/projections")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

func TestProjectionReconcile_NoStores(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/projections/reconcile", testTenantID,
		map[string]any{"projection": "search"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}
