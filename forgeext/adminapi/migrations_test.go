package adminapi

import (
	"encoding/json"
	"net/http"
	"testing"
)

// The fake-backed harness constructs the Extension with a nil *forgeext.Extension
// parent (see fakeBackedAdminExt), so there is no real migration target (no DSN,
// no grove.DB) to report status against. handleMigrationStatus must degrade to
// 501 rather than panic on the nil parent — mirroring how the projections status
// endpoint (TestProjections_NotAvailableOnFake) tolerates the fake backend.
func TestMigrationStatus_ReadOnly(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF; status is still available (read-only, ungated)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/migrations")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 (no migration target on the fake-backed harness)", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] == "" {
		t.Fatalf("expected a non-empty error message in the 501 body")
	}
}
