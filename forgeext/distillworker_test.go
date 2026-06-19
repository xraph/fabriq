package forgeext

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/internal/metrics"
)

// dwNote is a small distillable grove model for the distill-worker test:
// title/body are the L0 source text and site_id is the scope.
type dwNote struct {
	grove.BaseModel `grove:"table:dwnotes"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Title    string `grove:"title,notnull"`
	Body     string `grove:"body"`
	SiteID   string `grove:"site_id"`
}

// fakeSum is a deterministic summarizer: it concatenates the raw L0 text or the
// child summaries. No model call — keeps the test hermetic.
type fakeSum struct{}

func (fakeSum) Summarize(_ context.Context, in agent.SummaryInput) (string, error) {
	if len(in.Children) > 0 {
		out := "rollup"
		for _, c := range in.Children {
			out += "|" + c.Summary
		}
		return out, nil
	}
	return "l0(" + string(in.Raw) + ")", nil
}

// distillReg registers a distillable "note" (title+body, scoped by site) plus
// the digest_node tree entity (domain.DigestNode — forgeext may import domain).
func distillReg(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "note", Kind: registry.KindAggregate, Model: (*dwNote)(nil),
		Distill: &registry.DistillSpec{SourceFields: []string{"title", "body"}, Scopes: []string{"site"}},
	})
	r.MustRegister(registry.EntitySpec{
		Name: agent.DigestEntity, Kind: registry.KindAggregate, Model: (*domain.DigestNode)(nil),
		GraphNode: "DigestNode",
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHasDistillableEntity(t *testing.T) {
	if !hasDistillableEntity(distillReg(t)) {
		t.Fatal("want true for a registry with a Distill entity")
	}
	plain := registry.New()
	plain.MustRegister(registry.EntitySpec{Name: "p", Kind: registry.KindAggregate, Model: (*dwNote)(nil)})
	_ = plain.Validate()
	if hasDistillableEntity(plain) {
		t.Fatal("want false for a registry with no Distill entity")
	}
}

func TestDistillSweeper_MarkSweepBuildsTree(t *testing.T) {
	reg := distillReg(t) // note (distillable) + digest_node
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := agent.NewDistiller(fab, reg, stubEmb{}, fakeSum{}, nil, cas, agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	sw := NewDistillSweeper(d, 5*time.Millisecond, nil)
	handle := distillHandler(context.Background(), sw)

	env := event.Envelope{TenantID: "acme", Aggregate: "note", AggID: "n1", Type: "note.created",
		Payload: json.RawMessage(`{"id":"n1","title":"Pump","body":"vibration","site_id":"s1"}`)}
	if err := handle("s-1", env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Wait for the debounced sweep, then assert the L0 digest's VECTOR exists
	// (mirrors the embed worker test — no relational seeding needed).
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var matches []query.VectorMatch
		_ = w.Vector.Similar(ctx, query.VectorQuery{Entity: agent.DigestEntity, Embedding: []float32{1, 0, 0}, K: 50}, &matches)
		for _, m := range matches {
			if m.ID == agent.L0ID("note", "n1") {
				return // success: the debounced sweep built the L0 digest
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("debounced sweep did not build the L0 digest node")
}

// TestDistillSweeper_IncrementsMetrics asserts that a sweep driven through
// distillHandler increments fabriq_distill_nodes_total and
// fabriq_distill_summaries_total via the distillMetricsObserver.
func TestDistillSweeper_IncrementsMetrics(t *testing.T) {
	reg := distillReg(t) // note (distillable) + digest_node
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := agent.NewDistiller(fab, reg, stubEmb{}, fakeSum{}, nil, cas, agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}

	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}

	// Non-nil metrics wires the distillMetricsObserver into the Distiller.
	sw := NewDistillSweeper(d, 5*time.Millisecond, m)
	handle := distillHandler(context.Background(), sw)

	env := event.Envelope{
		TenantID:  "acme",
		Aggregate: "note",
		AggID:     "n1",
		Type:      "note.created",
		Payload:   json.RawMessage(`{"id":"n1","title":"Pump","body":"vibration","site_id":"s1"}`),
	}
	if err := handle("s-1", env); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Poll for the debounced sweep (mirrors TestDistillSweeper_MarkSweepBuildsTree).
	// DistillNodesTotal > 0 proves NodeBuilt() was called, i.e. the observer fired.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(m.DistillNodesTotal) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.DistillNodesTotal); got == 0 {
		t.Fatalf("DistillNodesTotal = 0 after sweep; observer not wired or sweep did not run")
	}
	if got := testutil.ToFloat64(m.DistillSummariesTotal); got == 0 {
		t.Fatalf("DistillSummariesTotal = 0 after sweep; Summarized() observer callback not fired")
	}

	// Verify the vector side-effect also happened (belt-and-suspenders: matches
	// the existing build test so we know it's the same sweep code path).
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	var matches []query.VectorMatch
	_ = w.Vector.Similar(ctx, query.VectorQuery{Entity: agent.DigestEntity, Embedding: []float32{1, 0, 0}, K: 50}, &matches)
	found := false
	for _, mv := range matches {
		if mv.ID == agent.L0ID("note", "n1") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("L0 digest node vector not found; sweep may have run but distillation failed silently")
	}
}
