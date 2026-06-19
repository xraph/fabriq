package forgeext

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
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
