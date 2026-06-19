// core/agent/recall_graph_rank_test.go
package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestTopSeeds_TakesHighestFusedFirst(t *testing.T) {
	fused := []scoredRef{
		{ref: ref{Entity: "e", ID: "a"}, score: 0.9},
		{ref: ref{Entity: "e", ID: "b"}, score: 0.5},
		{ref: ref{Entity: "e", ID: "c"}, score: 0.1},
	}
	got := topSeeds(fused, 2)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("want [a b], got %+v", got)
	}
	if all := topSeeds(fused, 0); len(all) != 3 {
		t.Fatalf("n<=0 → no cap, want 3 got %d", len(all))
	}
}

// With GraphSeeds=1, the single expanded seed must be the highest FUSED-score
// candidate, not merely the first vector hit.
func TestRecall_GraphSeedsRankOrdered(t *testing.T) {
	reg := graphRegistry(t) // gasset -LOCATED_AT-> gsite
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3, GraphSeeds: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	sTop, _ := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "top"}})
	sLow, _ := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "low"}})
	aTop, _ := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "top", SiteID: sTop.AggID}})
	aLow, _ := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "low", SiteID: sLow.AggID}})

	// aLow is a vector hit; aTop is BOTH a vector hit and a search hit → higher fused score.
	if err := w.Vector.Upsert(ctx, "gasset", aTop.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "gasset", aLow.AggID, []float32{0.9, 0.1, 0}, nil); err != nil {
		t.Fatal(err)
	}
	// graph cann for BOTH assets' forward edge; only aTop's is expected to run
	w.Graph.Cann(expansionCypher("GAsset", "LOCATED_AT", "GSite", 1, false), []string{sTop.AggID})

	// gasset is NOT searchable in graphRegistry, so search contributes nothing here;
	// fused order == vector order, and aTop (cosine 1.0) outranks aLow (0.9).
	pack, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100000, Entities: []string{"gasset"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	// With GraphSeeds=1, only aTop is expanded → sTop present, sLow absent.
	if !hasItem(pack, "gsite", sTop.AggID) {
		t.Fatalf("want sTop (neighbour of top-ranked seed); items=%+v", pack.Items)
	}
	if hasItem(pack, "gsite", sLow.AggID) {
		t.Fatalf("sLow should NOT be expanded (aLow below seed cap); items=%+v", pack.Items)
	}
	_ = aLow
}
