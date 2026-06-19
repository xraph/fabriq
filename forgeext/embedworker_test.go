package forgeext

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/internal/metrics"
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
	handle := embedHandler(context.Background(), ix, nil)

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
	handle := embedHandler(context.Background(), ix, nil)
	// no TenantID → handler skips (returns nil, indexes nothing) rather than erroring the consumer
	if err := handle("s", event.Envelope{Aggregate: "ewdoc", AggID: "x", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"t"}`)}); err != nil {
		t.Fatalf("tenant-less event should be skipped, got %v", err)
	}
}

func TestEmbedHandler_UnindexablePayloadSkipped(t *testing.T) {
	reg := embedReg(t)
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	ix, err := agent.NewIndexer(fab, reg, stubEmb{})
	if err != nil {
		t.Fatal(err)
	}
	handle := embedHandler(context.Background(), ix, nil)

	// Malformed JSON payload for an embeddable entity with a valid tenant id.
	env := event.Envelope{
		TenantID:  "acme",
		Aggregate: "ewdoc",
		AggID:     "bad1",
		Type:      "ewdoc.created",
		Payload:   json.RawMessage(`{not json`),
	}
	if err := handle("stream-bad", env); err != nil {
		t.Fatalf("unindexable payload should be ack-skipped (nil), got %v", err)
	}

	// Nothing should have been indexed under the tenant.
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	var matches []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "ewdoc", Embedding: []float32{1, 0, 0}, K: 10}, &matches); err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		if m.ID == "bad1" {
			t.Fatalf("poison event should not have been indexed; got match %+v", m)
		}
	}
}

func TestWithEmbedder_SetsConfig(t *testing.T) {
	var c Config
	WithEmbedder(stubEmb{})(&c)
	if c.Embedder == nil {
		t.Fatal("WithEmbedder did not set Config.Embedder")
	}
}

type errEmb struct{}

func (errEmb) Dims() int { return 3 }
func (errEmb) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return nil, errors.New("embedder down")
}

func TestEmbedHandler_Metrics(t *testing.T) {
	reg := embedReg(t)
	w := fabriqtest.NewWorld(reg)
	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}

	// success → EmbedEventsTotal++
	ixOK, _ := agent.NewIndexer(fabriqtest.NewFabric(w), reg, stubEmb{})
	hOK := embedHandler(context.Background(), ixOK, m)
	if err := hOK("s", event.Envelope{TenantID: "acme", Aggregate: "ewdoc", AggID: "d1", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"hi"}`)}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.EmbedEventsTotal); got != 1 {
		t.Fatalf("EmbedEventsTotal = %v, want 1", got)
	}

	// transient failure → EmbedFailuresTotal++ and error propagates
	ixErr, _ := agent.NewIndexer(fabriqtest.NewFabric(fabriqtest.NewWorld(reg)), reg, errEmb{})
	hErr := embedHandler(context.Background(), ixErr, m)
	if err := hErr("s", event.Envelope{TenantID: "acme", Aggregate: "ewdoc", AggID: "d2", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"x"}`)}); err == nil {
		t.Fatal("want error from failing embedder")
	}
	if got := testutil.ToFloat64(m.EmbedFailuresTotal); got != 1 {
		t.Fatalf("EmbedFailuresTotal = %v, want 1", got)
	}
}

// TestEmbedHandler_NilMetrics verifies that passing nil metrics does not panic.
func TestEmbedHandler_NilMetrics(t *testing.T) {
	reg := embedReg(t)
	ix, _ := agent.NewIndexer(fabriqtest.NewFabric(fabriqtest.NewWorld(reg)), reg, stubEmb{})
	h := embedHandler(context.Background(), ix, nil)
	if err := h("s", event.Envelope{TenantID: "acme", Aggregate: "ewdoc", AggID: "d", Type: "ewdoc.created", Payload: json.RawMessage(`{"title":"t"}`)}); err != nil {
		t.Fatal(err)
	}
}
