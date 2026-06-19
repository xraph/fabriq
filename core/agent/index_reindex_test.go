// core/agent/index_reindex_test.go
package agent

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
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

// TestIndexer_ReindexPaginates verifies that Reindex iterates across multiple
// pages when the total row count exceeds reindexBatch.
func TestIndexer_ReindexPaginates(t *testing.T) {
	old := reindexBatch
	reindexBatch = 2
	defer func() { reindexBatch = old }()

	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}})
	ctx := testCtx(t, "acme")

	// Seed 5 rows — 3 pages at batch=2 (pages of 2, 2, 1).
	var ids []string
	for _, title := range []string{"p", "q", "r", "s", "u"} {
		res, err := ff.Exec(ctx, command.Command{Entity: "ixdoc", Op: command.OpCreate, Payload: &ixDoc{Title: title, Body: "body"}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.AggID)
	}

	n, err := ix.Reindex(ctx, "ixdoc")
	if err != nil {
		t.Fatalf("Reindex: %v", err)
	}
	if n != 5 {
		t.Fatalf("want 5 indexed, got %d", n)
	}
	for _, id := range ids {
		if !indexed(t, w, ctx, "ixdoc", id, []float32{1, 0, 0}) {
			t.Fatalf("id %q not indexed after paginated Reindex", id)
		}
	}
}

type countingEmbedder struct {
	calls   int
	batches []int
}

func (c *countingEmbedder) Dims() int { return 3 }
func (c *countingEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	c.calls++
	c.batches = append(c.batches, len(texts))
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0, 0}
	}
	return out, nil
}

func TestIndexer_ReindexBatchesEmbedCalls(t *testing.T) {
	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	emb := &countingEmbedder{}
	ix, _ := NewIndexer(ff, reg, emb)
	ctx := testCtx(t, "acme")

	orig := reindexBatch
	reindexBatch = 2
	defer func() { reindexBatch = orig }()

	for _, ttl := range []string{"a", "b", "c", "d", "e"} {
		if _, err := ff.Exec(ctx, command.Command{Entity: "ixdoc", Op: command.OpCreate, Payload: &ixDoc{Title: ttl, Body: "x"}}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := ix.Reindex(ctx, "ixdoc")
	if err != nil {
		t.Fatalf("Reindex: %v", err)
	}
	if n != 5 {
		t.Fatalf("want 5 indexed, got %d", n)
	}
	// 5 rows / batch 2 = 3 pages → 3 Embed calls (NOT 5), batch sizes 2,2,1
	if emb.calls != 3 {
		t.Fatalf("want 3 batched Embed calls, got %d (batches=%v)", emb.calls, emb.batches)
	}
}

// TestIndexer_ReindexDynamicEntity verifies that Reindex works for dynamic
// (DynamicSchema) entities, exercising the map-native branch of listVals.
func TestIndexer_ReindexDynamicEntity(t *testing.T) {
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "ditem", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_ditems",
			Columns: []registry.DynamicColumn{
				{Name: "label", Type: registry.ColText, NotNull: true},
				{Name: "note", Type: registry.ColText},
			},
		},
		Embed: &registry.EmbedSpec{Fields: []string{"label", "note"}},
	})
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{0, 1, 0}})
	ctx := testCtx(t, "acme")

	var ids []string
	for _, label := range []string{"alpha", "beta"} {
		res, err := ff.Exec(ctx, command.Command{
			Entity:  "ditem",
			Op:      command.OpCreate,
			Payload: map[string]any{"label": label, "note": "n"},
		})
		if err != nil {
			t.Fatalf("seed dynamic row: %v", err)
		}
		ids = append(ids, res.AggID)
	}

	n, err := ix.Reindex(ctx, "ditem")
	if err != nil {
		t.Fatalf("Reindex dynamic: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 indexed, got %d", n)
	}
	for _, id := range ids {
		if !indexed(t, w, ctx, "ditem", id, []float32{0, 1, 0}) {
			t.Fatalf("dynamic id %q not indexed after Reindex", id)
		}
	}
}
