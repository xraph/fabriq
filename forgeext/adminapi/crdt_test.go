package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// docNoteSpec returns a KindDocument entity that opts into the CRDT plane, so
// the registry-derived crdtConfigured() signal reports the plane as present.
func docNoteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "note", Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt"},
		Schema: &registry.DynamicSchema{
			Table:   "ds_notes",
			Columns: []registry.DynamicColumn{{Name: "body", Type: registry.ColText}},
		},
	}
}

// buildDocWorld registers a KindDocument "note" entity (so the CRDT plane reads
// as configured) on top of the standard fake world. The fake document store is
// the deferred stub, so reads against it still answer ErrStoreNotConfigured —
// which is exactly what proves the store-level 501 fallback.
func buildDocWorld(t *testing.T) *fabriqtest.World {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Register(docNoteSpec()); err != nil {
		t.Fatalf("register note: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return fabriqtest.NewWorld(reg)
}

// TestCrdtSnapshot_NotConfigured_NoDocumentEntity verifies that GET
// /admin/crdt/:entity/:id returns 501 when NO KindDocument entity is registered
// (the registry-derived plane signal is false), without ever touching the
// document store.
func TestCrdtSnapshot_NotConfigured_NoDocumentEntity(t *testing.T) {
	world := buildTestWorld(t) // widget-only: no document plane
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc123")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
	assertNotConfiguredBody(t, resp)
}

// TestCrdtSnapshot_LiveFakeStore verifies that with a KindDocument entity
// registered, a Snapshot against the (now live, in-memory) fake store
// serves an empty document state for an unknown id.
func TestCrdtSnapshot_LiveFakeStore(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc123")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
}

// TestCrdtUpdates_NotConfigured_NoDocumentEntity verifies the update-log route
// returns 501 when no document plane is registered.
func TestCrdtUpdates_NotConfigured_NoDocumentEntity(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc123/updates")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
	assertNotConfiguredBody(t, resp)
}

// TestCrdtUpdates_LiveFakeStore verifies the update-log route serves an
// empty log for an unknown id against the (now live, in-memory) fake store.
func TestCrdtUpdates_LiveFakeStore(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc123/updates")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
}

// TestCrdtUpdates_BadLimit verifies the update-log route validates ?limit=
// before touching the store: with the document plane registered, a non-numeric
// limit yields 400 (parameter validation runs after the plane check but before
// the Sync call).
func TestCrdtUpdates_BadLimit(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc123/updates?limit=notanumber")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, body)
	}
}

// TestCrdtRead_InMeta verifies the static capability list advertises crdt.read.
func TestCrdtRead_InMeta(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, c := range got.Capabilities {
		if c == "crdt.read" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("meta capabilities %v must include crdt.read", got.Capabilities)
	}
}

// assertNotConfiguredBody checks the 501 response carries the stable
// not-configured error payload the SPA branches on.
func assertNotConfiguredBody(t *testing.T, resp *http.Response) {
	t.Helper()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode 501 body: %v", err)
	}
	if body["error"] == "" {
		t.Errorf("501 body must carry an 'error' field, got %v", body)
	}
}
