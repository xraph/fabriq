// core/agent/index_reindex_test.go
package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestIndexer_ReindexBackfillsAllRows(t *testing.T) {
	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}})
	ctx := testCtx(t, "acme")

	// Seed 3 ixdoc rows via the command path (vectors NOT auto-written — no wiring).
	var ids []string
	for _, title := range []string{"a", "b", "c"} {
		res, err := ff.Exec(ctx, command.Command{Entity: "ixdoc", Op: command.OpCreate, Payload: &ixDoc{Title: title, Body: "body"}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.AggID)
	}
	// Before backfill: nothing indexed.
	if indexed(t, w, ctx, "ixdoc", ids[0], []float32{1, 0, 0}) {
		t.Fatal("expected empty vector store before Reindex")
	}

	n, err := ix.Reindex(ctx, "ixdoc")
	if err != nil {
		t.Fatalf("Reindex: %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 indexed, got %d", n)
	}
	for _, id := range ids {
		if !indexed(t, w, ctx, "ixdoc", id, []float32{1, 0, 0}) {
			t.Fatalf("id %q not indexed after Reindex", id)
		}
	}
}

func TestIndexer_ReindexNonEmbeddableIsZero(t *testing.T) {
	reg := embedRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}})
	ctx := testCtx(t, "acme")
	n, err := ix.Reindex(ctx, "plain")
	if err != nil || n != 0 {
		t.Fatalf("want 0 indexed / nil err for non-embeddable, got %d / %v", n, err)
	}
}
