package adminapi

import (
	"net/http"
	"testing"
)

// The outbox event-log happy path reads raw SQL from fabriq_outbox, which the
// in-memory FakeRelational deliberately does not execute (it errors on raw
// Query) — so that path is covered by the live admin-demo verification, mirror
// of the timeseries endpoints. These unit tests cover the request-validation
// branches, which return before any query runs.

func TestListEvents_InvalidPublished(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/events?published=bogus")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListEvents_InvalidLimit(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/events?limit=0")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
