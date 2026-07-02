package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/document"
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
