package adminapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/projection"
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

// failingStateRepo is a minimal projection.StateRepo whose Get always fails
// with driver-shaped text, standing in for a real *postgres.StateRepo hitting
// a driver fault outside the inTenantTx classify seam (see
// adapters/postgres/state.go StateRepo.Get).
type failingStateRepo struct{ err error }

func (f failingStateRepo) AppliedVersion(context.Context, string, string, string, string) (int64, error) {
	return 0, f.err
}
func (f failingStateRepo) Get(context.Context, string, string) (projection.State, error) {
	return projection.State{}, f.err
}
func (f failingStateRepo) Upsert(context.Context, projection.State) error { return f.err }

// TestProjections_StateRepoFailure_RendersStructuredError verifies that a
// StateRepo.Get failure carrying raw driver-shaped text (as
// *postgres.StateRepo.Get returns when it hits a driver fault outside
// inTenantTx's classify seam) is routed through renderError instead of the
// old forge.InternalError, so the response is the structured errorBody shape
// and never leaks the raw driver text in the body — mirroring
// TestAdminPlugins_Create_ExecFailure_RendersStructuredError's leak probe.
func TestProjections_StateRepoFailure_RendersStructuredError(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)

	const driverLeakText = "fabriq: projection state: pgdriver: SQLSTATE 08006 connection failure"
	e.stateRepo = failingStateRepo{err: errors.New(driverLeakText)}

	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/projections")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", resp.StatusCode, body)
	}
	if strings.Contains(string(body), "pgdriver") || strings.Contains(string(body), "SQLSTATE") {
		t.Fatalf("response body leaked raw driver text: %s", body)
	}
}

func TestProjectionReconcile_NoStores(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/projections/reconcile",
		map[string]any{"projection": "search"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}
