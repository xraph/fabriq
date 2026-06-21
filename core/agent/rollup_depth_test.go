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

func isIntermediateID(id string) bool { return containsHash(id) }
func containsHash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' {
			return true
		}
	}
	return false
}
