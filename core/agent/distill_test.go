package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// distillRegistry registers one distillable entity "note" scoped by "site",
// plus the digest_node tree entity.
func distillRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "note", Kind: registry.KindAggregate, Model: (*tDoc)(nil),
		Distill: &registry.DistillSpec{SourceFields: []string{"title", "body"}, Scopes: []string{"site"}},
	})
	r.MustRegister(registry.EntitySpec{
		Name: "digest_node", Kind: registry.KindAggregate, Model: (*digestModel)(nil),
		GraphNode: "DigestNode",
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func newDistiller(t testing.TB, r *registry.Registry, cas *fabriqtest.FakeCAS, sum Summarizer, g Guard) (*Distiller, *fabriqtest.World) {
	t.Helper()
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, sum, g, cas, DistillConfig{VectorDims: 3, RecipeVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	return d, w
}

func TestDistillL0_SummarizesAndShortCircuits(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, nil)
	ctx := testCtx(t, "acme")

	vals := map[string]any{"id": "n1", "title": "Pump", "body": "vibration high"}
	changed, err := d.DistillL0(ctx, "note", "n1", vals)
	if err != nil || !changed {
		t.Fatalf("first distill: changed=%v err=%v", changed, err)
	}
	if sum.calls != 1 {
		t.Fatalf("expected 1 summarize call, got %d", sum.calls)
	}
	if cas.Len() != 1 {
		t.Fatalf("expected 1 summary blob, got %d", cas.Len())
	}

	// Re-distilling identical source must short-circuit (no LLM call).
	changed, err = d.DistillL0(ctx, "note", "n1", vals)
	if err != nil || changed {
		t.Fatalf("re-distill should short-circuit: changed=%v err=%v", changed, err)
	}
	if sum.calls != 1 {
		t.Fatalf("short-circuit must not call summarizer again, got %d", sum.calls)
	}

	// A source change must re-summarize and re-store.
	vals["body"] = "vibration low"
	changed, err = d.DistillL0(ctx, "note", "n1", vals)
	if err != nil || !changed {
		t.Fatalf("changed source must re-distill: changed=%v err=%v", changed, err)
	}
	if sum.calls != 2 {
		t.Fatalf("changed source must call summarizer again, got %d", sum.calls)
	}
}

func TestDistillL0_NonDistillableIsNoop(t *testing.T) {
	r := distillRegistry(t)
	d, _ := newDistiller(t, r, fabriqtest.NewFakeCAS(), &fakeSummarizer{}, nil)
	changed, err := d.DistillL0(testCtx(t, "acme"), "digest_node", "x", map[string]any{"id": "x"})
	if err != nil || changed {
		t.Fatalf("non-distillable entity must be a no-op: changed=%v err=%v", changed, err)
	}
}

func TestDistillL0_EmptyTextIsNoop(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, nil)
	changed, err := d.DistillL0(testCtx(t, "acme"), "note", "n1", map[string]any{"id": "n1"})
	if err != nil || changed {
		t.Fatalf("empty source text must be a no-op: changed=%v err=%v", changed, err)
	}
	if sum.calls != 0 || cas.Len() != 0 {
		t.Fatalf("empty source must not summarize or store: calls=%d blobs=%d", sum.calls, cas.Len())
	}
}

// A guard block at the emit stage is fail-closed: nothing is summarized into
// CAS and the node is not written.
func TestDistillL0_GuardBlockFailsClosed(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, w := newDistiller(t, r, cas, sum, newFakeGuard())
	ctx := testCtx(t, "acme")

	// Body contains SECRET -> emit-stage guard blocks (the summary echoes it).
	vals := map[string]any{"id": "n1", "title": "Pump", "body": "the SECRET key"}
	changed, err := d.DistillL0(ctx, "note", "n1", vals)
	if err != nil {
		t.Fatalf("guard block should not error: %v", err)
	}
	if changed {
		t.Fatalf("guard block must return changed=false")
	}
	if cas.Len() != 0 {
		t.Fatalf("fail-closed must store nothing in CAS, got %d", cas.Len())
	}
	// No digest node row was written.
	ent, _ := r.Get(DigestEntity)
	model := ent.Binding.NewModel()
	if err := w.Rel.Get(ctx, DigestEntity, L0ID("note", "n1"), model); err == nil {
		t.Fatalf("fail-closed must not persist a digest node")
	}
}

// The ingest-stage guard redaction is what the summarizer sees and what lands
// in CAS; raw PII never reaches the store.
func TestDistillL0_GuardRedactsBeforeSummary(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, newFakeGuard())
	ctx := testCtx(t, "acme")

	vals := map[string]any{"id": "n1", "title": "Pump", "body": "ssn 123456789"}
	changed, err := d.DistillL0(ctx, "note", "n1", vals)
	if err != nil || !changed {
		t.Fatalf("redacted distill: changed=%v err=%v", changed, err)
	}
	if cas.Len() != 1 {
		t.Fatalf("expected 1 redacted blob, got %d", cas.Len())
	}
}
