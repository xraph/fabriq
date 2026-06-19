package forgeext

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type ewDoc struct {
	grove.BaseModel `grove:"table:ewdocs"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Title           string `grove:"title,notnull"`
}

type stubEmb struct{}

func (stubEmb) Dims() int { return 3 }
func (stubEmb) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0, 0}
	}
	return out, nil
}

func embedReg(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "ewdoc", Kind: registry.KindAggregate, Model: (*ewDoc)(nil),
		Embed: &registry.EmbedSpec{Fields: []string{"title"}},
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHasEmbeddableEntity(t *testing.T) {
	if !hasEmbeddableEntity(embedReg(t)) {
		t.Fatal("want true for a registry with an Embed entity")
	}
	plain := registry.New()
	plain.MustRegister(registry.EntitySpec{Name: "p", Kind: registry.KindAggregate, Model: (*ewDoc)(nil)})
	_ = plain.Validate()
	if hasEmbeddableEntity(plain) {
		t.Fatal("want false for a registry with no Embed entity")
	}
}

func TestEmbedHandler_IndexesEventTenantScoped(t *testing.T) {
	reg := embedReg(t)
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	ix, err := agent.NewIndexer(fab, reg, stubEmb{})
	if err != nil {
		t.Fatal(err)
	}
	handle := embedHandler(context.Background(), ix)

	env := event.Envelope{TenantID: "acme", Aggregate: "ewdoc", AggID: "d1", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"hello"}`)}
	if err := handle("stream-1", env); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// the vector was upserted under tenant acme
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	var matches []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "ewdoc", Embedding: []float32{1, 0, 0}, K: 10}, &matches); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range matches {
		if m.ID == "d1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("event not indexed under tenant acme; matches=%+v", matches)
	}
}

func TestEmbedHandler_TenantlessEventSkipped(t *testing.T) {
	reg := embedReg(t)
	w := fabriqtest.NewWorld(reg)
	ix, _ := agent.NewIndexer(fabriqtest.NewFabric(w), reg, stubEmb{})
	handle := embedHandler(context.Background(), ix)
	// no TenantID → handler skips (returns nil, indexes nothing) rather than erroring the consumer
	if err := handle("s", event.Envelope{Aggregate: "ewdoc", AggID: "x", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"t"}`)}); err != nil {
		t.Fatalf("tenant-less event should be skipped, got %v", err)
	}
}

func TestWithEmbedder_SetsConfig(t *testing.T) {
	var c Config
	WithEmbedder(stubEmb{})(&c)
	if c.Embedder == nil {
		t.Fatal("WithEmbedder did not set Config.Embedder")
	}
}
