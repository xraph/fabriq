// core/agent/index_test.go
package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

type ixDoc struct {
	grove.BaseModel `grove:"table:ixdocs"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Title           string `grove:"title,notnull"`
	Body            string `grove:"body"`
}

func embedRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "ixdoc", Kind: registry.KindAggregate, Model: (*ixDoc)(nil),
		Embed: &registry.EmbedSpec{Fields: []string{"title", "body"}},
	})
	r.MustRegister(registry.EntitySpec{ // NOT embeddable
		Name: "plain", Kind: registry.KindAggregate, Model: (*tDoc)(nil),
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

// indexed reports whether the vector store has an entry for entity/id near vec.
func indexed(t testing.TB, w *fabriqtest.World, ctx context.Context, entity, id string, vec []float32) bool {
	t.Helper()
	var matches []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: entity, Embedding: vec, K: 50}, &matches); err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		if m.ID == id {
			return true
		}
	}
	return false
}

func TestIndexer_IndexRowEmbedsAndUpserts(t *testing.T) {
	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	emb := stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}
	ix, err := NewIndexer(ff, reg, emb)
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	if err := ix.IndexRow(ctx, "ixdoc", "d1", map[string]any{"title": "alpha", "body": "beta"}); err != nil {
		t.Fatalf("IndexRow: %v", err)
	}
	if !indexed(t, w, ctx, "ixdoc", "d1", []float32{1, 0, 0}) {
		t.Fatal("d1 not in vector store after IndexRow")
	}
}

func TestIndexer_IndexRowNoopForNonEmbeddable(t *testing.T) {
	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}})
	ctx := testCtx(t, "acme")

	if err := ix.IndexRow(ctx, "plain", "p1", map[string]any{"title": "x"}); err != nil {
		t.Fatalf("non-embeddable IndexRow should be a no-op, got %v", err)
	}
	if indexed(t, w, ctx, "plain", "p1", []float32{1, 0, 0}) {
		t.Fatal("non-embeddable entity must not be indexed")
	}
}

func TestIndexer_IndexEventCreateAndSkipDelete(t *testing.T) {
	reg := embedRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	ix, _ := NewIndexer(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}})
	ctx := testCtx(t, "acme")

	created := event.Envelope{Aggregate: "ixdoc", AggID: "e1", Type: "ixdoc.created", Payload: json.RawMessage(`{"title":"hi","body":"there"}`)}
	if err := ix.IndexEvent(ctx, created); err != nil {
		t.Fatalf("IndexEvent created: %v", err)
	}
	if !indexed(t, w, ctx, "ixdoc", "e1", []float32{1, 0, 0}) {
		t.Fatal("created event not indexed")
	}

	del := event.Envelope{Aggregate: "ixdoc", AggID: "e2", Type: "ixdoc.deleted", Payload: json.RawMessage(`{"title":"gone"}`)}
	if err := ix.IndexEvent(ctx, del); err != nil {
		t.Fatalf("IndexEvent delete: %v", err)
	}
	if indexed(t, w, ctx, "ixdoc", "e2", []float32{1, 0, 0}) {
		t.Fatal("deleted event must be skipped (no vector delete in v1)")
	}
}

func TestNewIndexer_RequiresEmbedder(t *testing.T) {
	reg := embedRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	if _, err := NewIndexer(ff, reg, nil); err == nil {
		t.Fatal("want error for nil embedder")
	}
}

