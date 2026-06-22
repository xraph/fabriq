package conformance

import (
	"testing"

	"github.com/xraph/fabriq/core/query"
)

// emb builds a test embedding of the required dimension. x and y are placed at
// indices 0 and 1 (or the only index for dim=1); remaining components are zero.
// dim=0 means "no constraint" and falls back to 3 (enough for cosine ordering).
func emb(dim int, x, y float32) []float32 {
	if dim <= 0 {
		dim = 3
	}
	v := make([]float32, dim)
	v[0] = x
	if dim > 1 {
		v[1] = y
	}
	return v
}

// RunVector exercises the VectorQuerier port: upsert/similar ordering, get
// hit/miss, point delete, metadata-filtered similar, and delete-by-meta.
// Skipped when the backend does not implement the vector port.
func RunVector(t *testing.T, b Backend) {
	t.Helper()
	env := b.Setup(t)
	if env.Vector == nil {
		t.Skipf("conformance: %s does not implement the vector port", b.Name())
		return
	}

	ctx := env.Ctx
	vec := env.Vector
	dim := env.EmbeddingDim

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Seed two embeddings with distinct metadata.
	must(vec.Upsert(ctx, "doc", "a", emb(dim, 1, 0), map[string]any{"kind": "note"}))
	must(vec.Upsert(ctx, "doc", "b", emb(dim, 0, 1), map[string]any{"kind": "task"}))

	// Similar: nearest to [1,0,…] should be "a" (cosine sim = 1.0 vs 0.0).
	var top []query.VectorMatch
	must(vec.Similar(ctx, query.VectorQuery{Entity: "doc", Embedding: emb(dim, 1, 0), K: 2}, &top))
	if len(top) == 0 || top[0].ID != "a" {
		t.Fatalf("conformance: %s: Similar top = %+v, want a first", b.Name(), top)
	}

	// Filtered Similar: only the "task" entry should appear.
	var filtered []query.VectorMatch
	must(vec.Similar(ctx, query.VectorQuery{
		Entity:    "doc",
		Embedding: emb(dim, 1, 0),
		K:         10,
		Filter:    map[string]string{"kind": "task"},
	}, &filtered))
	if len(filtered) != 1 || filtered[0].ID != "b" {
		t.Fatalf("conformance: %s: filtered Similar = %+v, want only b", b.Name(), filtered)
	}

	// Get hit: "a" should be present.
	if _, err := vec.Get(ctx, "doc", "a"); err != nil {
		t.Fatalf("conformance: %s: Get(a): %v", b.Name(), err)
	}

	// DeleteByMeta removes only embeddings matching the filter.
	must(vec.DeleteByMeta(ctx, "doc", map[string]string{"kind": "note"}))

	// "a" should now be gone.
	if _, err := vec.Get(ctx, "doc", "a"); err == nil {
		t.Fatalf("conformance: %s: a should be deleted by meta but Get returned nil error", b.Name())
	}

	// "b" should survive.
	if _, err := vec.Get(ctx, "doc", "b"); err != nil {
		t.Fatalf("conformance: %s: b should survive DeleteByMeta(kind=note): %v", b.Name(), err)
	}

	// Tenant isolation: a foreign tenant sees no embeddings.
	var foreign []query.VectorMatch
	must(vec.Similar(env.ForeignCtx, query.VectorQuery{
		Entity:    "doc",
		Embedding: emb(dim, 1, 0),
		K:         10,
	}, &foreign))
	if len(foreign) != 0 {
		t.Fatalf("conformance: %s: foreign tenant saw %d row(s), want 0", b.Name(), len(foreign))
	}
}
