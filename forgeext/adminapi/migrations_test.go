package adminapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestRenderMigrationScaffold(t *testing.T) {
	fn, content, err := renderMigrationScaffold("add_widget", "202607010001")
	if err != nil {
		t.Fatalf("renderMigrationScaffold: %v", err)
	}
	if fn != "add_widget.go" {
		t.Errorf("filename = %q, want add_widget.go", fn)
	}
	for _, want := range []string{
		"package migrations", "migrate.Migration{", `Name:    "add_widget"`,
		`Version: "202607010001"`, "Up: func", "Down: func",
		"execAll(ctx, exec, []string{", "migrationAddWidget",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("scaffold content missing %q", want)
		}
	}
	if _, _, err := renderMigrationScaffold("Bad Name!", "202607010001"); err == nil {
		t.Error("invalid name should error")
	}
	if _, _, err := renderMigrationScaffold("ok", "notdigits"); err == nil {
		t.Error("invalid version should error")
	}
}

func TestMigrationScaffold_403WhenGateOff(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/migrations/scaffold?name=x&version=1")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (gate off)", resp.StatusCode)
	}
}

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

// With the gate OFF, the execution endpoints must 403 before touching the parent.
func TestMigrateUp_403WhenGateOff(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/migrations/up", testTenantID, map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (gate off)", resp.StatusCode)
	}
}

// With the gate ON but the fake-backed nil parent, the execution endpoint must
// 501 (nil-guarded) — never panic, and never start a goroutine that dereferences
// the nil parent.
func TestMigrateUp_501WhenGateOnButNoParent(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithSchemaAdmin()) // gate ON, parent nil
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/migrations/up", testTenantID, map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 (nil parent, gate on)", resp.StatusCode)
	}
}

// Polling an unknown job id returns 404.
func TestMigrationJob_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/migrations/jobs/nope")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// Streaming an unknown job id returns 404 before opening the SSE stream.
func TestMigrationJobStream_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/migrations/jobs/nope/stream")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
