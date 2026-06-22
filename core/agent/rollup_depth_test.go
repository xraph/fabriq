package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func newDepthDistiller(t *testing.T, r *registry.Registry, cas *fabriqtest.FakeCAS, maxFanIn int) *Distiller {
	t.Helper()
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	d, err := NewDistiller(fab, r, variedEmbedder{dims: 8}, &fakeSummarizer{}, nil, cas,
		DistillConfig{VectorDims: 8, RecipeVersion: "v1", ClusterBits: 4, NoiseFloor: 2,
			MaxFanIn: maxFanIn, ClusterSubBits: 4, SummarizerInputBudget: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestAdaptiveDepth_SplitsOversizedScope: with MaxFanIn=2 and >2 notes in one
// scope, the scope must gain intermediate (#-suffixed) sub-cluster nodes.
func TestAdaptiveDepth_SplitsOversizedScope(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d := newDepthDistiller(t, r, cas, 2)
	ctx := testCtx(t)
	for i, body := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		id := string(rune('a' + i))
		if _, err := d.DistillL0(ctx, "note", id, map[string]any{"id": id, "title": id, "body": body, "site_id": "s1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	l1s, err := d.listNodes(ctx, LevelScope)
	if err != nil {
		t.Fatal(err)
	}
	intermediate := 0
	for _, n := range l1s {
		if isIntermediateID(n.ID) { // helper below: contains "#"
			intermediate++
		}
	}
	if intermediate == 0 {
		t.Fatalf("oversized scope (MaxFanIn=2, 6 notes) must produce intermediate nodes; got none")
	}
}

// TestAdaptiveDepth_DormantUnderCap: with a generous cap, no intermediate nodes
// appear — small tenants keep the flat 3-level shape.
func TestAdaptiveDepth_DormantUnderCap(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d := newDepthDistiller(t, r, cas, 64)
	ctx := testCtx(t)
	for i, body := range []string{"alpha", "beta", "gamma"} {
		id := string(rune('a' + i))
		if _, err := d.DistillL0(ctx, "note", id, map[string]any{"id": id, "title": id, "body": body, "site_id": "s1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	l1s, err := d.listNodes(ctx, LevelScope)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range l1s {
		if isIntermediateID(n.ID) {
			t.Fatalf("under-cap tenant must stay flat; found intermediate node %s", n.ID)
		}
	}
}

// TestAdaptiveDepth_GCsOrphanedIntermediate: build an oversized scope so
// intermediate "#" nodes form, then delete enough members that the geometry
// changes; the next Rollup must GC the now-orphaned intermediate(s).
func TestAdaptiveDepth_GCsOrphanedIntermediate(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d := newDepthDistiller(t, r, cas, 2) // MaxFanIn=2 → splits
	ctx := testCtx(t)
	ids := []string{"a", "b", "c", "d", "e", "f"}
	for i, body := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		if _, err := d.DistillL0(ctx, "note", ids[i], map[string]any{"id": ids[i], "title": ids[i], "body": body, "site_id": "s1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := d.listNodes(ctx, LevelScope)
	nInter := 0
	for _, n := range before {
		if isIntermediateID(n.ID) {
			nInter++
		}
	}
	if nInter == 0 {
		t.Fatal("setup: expected intermediate nodes after split")
	}
	// Delete most members to force a geometry change.
	for _, id := range []string{"a", "b", "c", "d"} {
		if _, err := d.DeleteL0(ctx, "note", id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := d.listNodes(ctx, LevelScope)
	for _, n := range after {
		if isIntermediateID(n.ID) {
			// any surviving intermediate must be one the final pass actually built
			// (i.e. still reachable). With only 2 members left and MaxFanIn=2, the
			// scope fits without splitting → zero intermediates should remain.
			t.Fatalf("orphaned intermediate %s was not GC'd", n.ID)
		}
	}
}
