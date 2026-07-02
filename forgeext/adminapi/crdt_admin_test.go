package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestCrdtEntities_ListsDocumentEntities(t *testing.T) {
	world := buildDocWorld(t) // existing helper that registers a KindDocument entity
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/entities")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, body)
	}
	var out struct {
		Items []struct {
			Entity string `json:"entity"`
			Kind   string `json:"kind"`
			Engine string `json:"engine"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) == 0 {
		t.Fatal("expected at least one document entity")
	}
	found := false
	for _, it := range out.Items {
		if it.Kind == "document" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no document-kind entity in %+v", out.Items)
	}
}

// TestCrdtSegments_ReturnsSeededSegments verifies GET
// /admin/crdt/:entity/:id/segments returns the sealed-history segment
// metadata seeded on the fake document store for a registered document
// entity.
func TestCrdtSegments_ReturnsSeededSegments(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	world.Docs.SeedSegments("note/abc", []document.SegmentInfo{
		{SegSeq: 1, SeqLo: 1, SeqHi: 2, UpdateCount: 2, ByteSize: 10},
	})
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc/segments")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 200 body=%s", resp.StatusCode, body)
	}
	var out struct {
		DocID string `json:"docId"`
		Items []struct {
			SeqLo       int64 `json:"seqLo"`
			SeqHi       int64 `json:"seqHi"`
			UpdateCount int64 `json:"updateCount"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.DocID != "note/abc" || len(out.Items) != 1 || out.Items[0].SeqHi != 2 {
		t.Fatalf("unexpected response %+v", out)
	}
}

// TestCrdtSegments_EmptyWhenNoSegments verifies that a registered document
// entity with no seeded segments returns 200 with an empty items list rather
// than a 404 or 501.
func TestCrdtSegments_EmptyWhenNoSegments(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/none/segments")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 200 body=%s", resp.StatusCode, body)
	}
	var out struct {
		DocID string `json:"docId"`
		Items []any  `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.DocID != "note/none" || len(out.Items) != 0 {
		t.Fatalf("want empty items for note/none, got %+v", out)
	}
}

// TestCrdtSegments_404ForAggregate verifies that segments are refused for a
// non-document (aggregate) entity, since sealed history segments only exist
// for the CRDT document plane.
func TestCrdtSegments_404ForAggregate(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/widget/w1/segments")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 404 body=%s", resp.StatusCode, body)
	}
}

// TestCrdtHistory_ReturnsSeededRange verifies GET
// /admin/crdt/:entity/:id/history returns the seeded raw update range for a
// registered document entity, in ascending seq order.
func TestCrdtHistory_ReturnsSeededRange(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	world.Docs.SeedHistory("note/abc", []document.HistoryUpdate{
		{Seq: 1, Update: json.RawMessage(`[{"field":"title"}]`)},
		{Seq: 2, Update: json.RawMessage(`[{"field":"body"}]`)},
	})
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc/history?from=1&to=2")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 200 body=%s", resp.StatusCode, body)
	}
	var out struct {
		DocID string `json:"docId"`
		Items []struct {
			Seq  int64 `json:"seq"`
			Size int   `json:"size"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 2 || out.Items[0].Seq != 1 || out.Items[1].Seq != 2 {
		t.Fatalf("unexpected history %+v", out)
	}
	if out.Items[0].Size <= 0 {
		t.Fatalf("expected non-zero size")
	}
}

// TestCrdtHistory_404ForAggregate verifies that history reads are refused for
// a non-document (aggregate) entity, matching the /segments 404 behavior.
func TestCrdtHistory_404ForAggregate(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/widget/w1/history")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 404 body=%s", resp.StatusCode, body)
	}
}

// TestCrdtHistory_BadRange verifies a non-integer "to" query param is
// rejected with 400 rather than silently ignored.
func TestCrdtHistory_BadRange(t *testing.T) {
	world := buildDocWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/note/abc/history?to=abc")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 400 body=%s", resp.StatusCode, body)
	}
}

// noteCrdtAggSpec returns a KindAggregate entity that ALSO carries a CRDTSpec.
//
// This looks unusual but is a deliberate, registry-sanctioned combination
// (registry.validateAndBind only rejects the reverse: KindDocument without a
// CRDTSpec). It is required to exercise the delete-purge wiring end-to-end
// through the real HTTP path in this fake:
//
//   - handleDeleteEntity's purge condition is
//     "ent.Spec.Kind == registry.KindDocument || ent.Spec.CRDT != nil" — i.e.
//     it fires for a CRDT-tagged entity regardless of Kind.
//   - command.Executor.prepare (core/command/validate.go) hard-rejects any
//     write whose Kind != KindAggregate ("the command plane only writes
//     aggregates (document writes go through the document plane)"), for BOTH
//     the fake and the real Postgres adapter.
//
// A pure KindDocument entity (e.g. the "note" spec in crdt_test.go) therefore
// can never be created/deleted via POST/DELETE {base}/entities through
// fab.Exec in the current architecture: creating it 400s (unknown to the
// command plane's aggregate path is not the failure — delete would 500, see
// below) — concretely, DELETE on a manually-seeded KindDocument row returns
// 500 (a plain, non-fabriqerr error from prepare), never 404 or 204. This was
// verified directly against buildDocWorld's "note" entity before writing this
// alternate spec; see the Task 7 report for the full investigation. Using a
// CRDT-tagged AGGREGATE lets the delete succeed (204) while still exercising
// the exact purge condition, without weakening any assertion.
func noteCrdtAggSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "crdtnote", Kind: registry.KindAggregate,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt"},
		Schema: &registry.DynamicSchema{
			Table:   "ds_crdtnotes",
			Columns: []registry.DynamicColumn{{Name: "body", Type: registry.ColText}},
		},
	}
}

// buildCrdtAggWorld builds a world with the widget aggregate plus the
// CRDT-tagged aggregate "crdtnote", so the purge condition's CRDT-spec arm can
// be exercised through a delete that actually reaches 204.
func buildCrdtAggWorld(t *testing.T) *fabriqtest.World {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Register(noteCrdtAggSpec()); err != nil {
		t.Fatalf("register crdtnote: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return fabriqtest.NewWorld(reg)
}

// TestDeleteDocumentEntity_PurgesHistory verifies that deleting a document
// (CRDT) entity via DELETE {base}/entities/:id?type=... best-effort purges its
// offloaded history (segments + raw update log) through document.HistoryPurger.
//
// See noteCrdtAggSpec's doc comment for why this uses a CRDT-tagged AGGREGATE
// rather than a pure KindDocument entity: command.Executor.prepare rejects any
// non-KindAggregate delete outright (500, not 404), so a pure KindDocument row
// can never reach 204 through this HTTP path in the current architecture. The
// purge condition itself (Kind == KindDocument || CRDT != nil) is exercised
// exactly the same way either way; this spec is the only one that lets the
// delete succeed so the purge call is actually reached.
func TestDeleteDocumentEntity_PurgesHistory(t *testing.T) {
	world := buildCrdtAggWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	postResp := postEntity(t, srv, map[string]any{
		"type": "crdtnote",
		"data": map[string]any{"body": "hello world"},
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("create status=%d body=%s", postResp.StatusCode, body)
	}
	created := decodeEntityItem(t, postResp)
	postResp.Body.Close()

	docID := "crdtnote/" + created.ID
	world.Docs.SeedHistory(docID, []document.HistoryUpdate{
		{Seq: 1, Update: json.RawMessage(`[]`)},
	})

	delResp := deleteEntity(t, srv, created.ID, "crdtnote")
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("delete status=%d want 204 body=%s", delResp.StatusCode, body)
	}

	if !world.Docs.DeletedHistory(docID) {
		t.Fatalf("expected DeleteHistory to be called for %q", docID)
	}
}

// TestDeleteAggregateEntity_DoesNotPurge verifies that deleting a plain
// (non-CRDT) aggregate entity does NOT attempt a history purge: the widget
// world has no document plane at all beyond the fake's deferred stub, and
// widget carries no CRDTSpec, so the purge condition never fires.
func TestDeleteAggregateEntity_DoesNotPurge(t *testing.T) {
	world := buildTestWorld(t) // widget-only aggregate world (no document plane)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	postResp := postEntity(t, srv, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Plain", "colour": "beige"},
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("create status=%d body=%s", postResp.StatusCode, body)
	}
	created := decodeEntityItem(t, postResp)
	postResp.Body.Close()

	docID := "widget/" + created.ID

	delResp := deleteEntity(t, srv, created.ID, "widget")
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("delete status=%d want 204 body=%s", delResp.StatusCode, body)
	}

	if world.Docs.DeletedHistory(docID) {
		t.Fatalf("expected DeleteHistory NOT to be called for %q", docID)
	}
}
