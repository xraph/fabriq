// core/agent/recall_graph_multihop_test.go
package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestExpansionCypher_FormsByHopsAndDirection(t *testing.T) {
	cases := []struct {
		hops    int
		reverse bool
		want    string
	}{
		{1, false, "MATCH (n:A {id: $id})-[:R]->(m:B) RETURN m.id"},
		{3, false, "MATCH (n:A {id: $id})-[:R*1..3]->(m:B) RETURN m.id"},
		{1, true, "MATCH (n:A {id: $id})<-[:R]-(m:B) RETURN m.id"},
		{2, true, "MATCH (n:A {id: $id})<-[:R*1..2]-(m:B) RETURN m.id"},
	}
	for _, c := range cases {
		if got := expansionCypher("A", "R", "B", c.hops, c.reverse); got != c.want {
			t.Fatalf("hops=%d reverse=%v:\n got %q\nwant %q", c.hops, c.reverse, got, c.want)
		}
	}
}

func TestRecall_GraphMultiHopForward(t *testing.T) {
	reg := graphRegistry(t) // gasset -LOCATED_AT-> gsite (from recall_graph_test.go)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	site, _ := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "S"}})
	asset, _ := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "A", SiteID: site.AggID}})
	if err := w.Vector.Upsert(ctx, "gasset", asset.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	// Hops=2 → variable-length cypher
	w.Graph.Cann(expansionCypher("GAsset", "LOCATED_AT", "GSite", 2, false), []string{site.AggID})

	pack, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100000, Entities: []string{"gasset"}, Hops: 2})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if !hasItem(pack, "gsite", site.AggID) {
		t.Fatalf("want gsite via 2-hop expansion; items=%+v warnings=%v", pack.Items, pack.Warnings)
	}
}

func TestRecall_GraphReverseExpansion(t *testing.T) {
	reg := graphRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	// reverse opt-in
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3, GraphReverse: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	site, _ := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "S"}})
	asset, _ := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "A", SiteID: site.AggID}})
	// seed the SITE (vector); reverse edge: gsite <-LOCATED_AT- gasset
	if err := w.Vector.Upsert(ctx, "gsite", site.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	w.Graph.Cann(expansionCypher("GSite", "LOCATED_AT", "GAsset", 1, true), []string{asset.AggID})

	pack, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100000, Entities: []string{"gsite"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if !hasItem(pack, "gasset", asset.AggID) {
		t.Fatalf("want gasset via reverse expansion; items=%+v warnings=%v", pack.Items, pack.Warnings)
	}
}

// hasItem reports whether the pack contains an item for (entity,id).
func hasItem(pack ContextPack, entity, id string) bool {
	for _, it := range pack.Items {
		if it.Entity == entity && it.ID == id {
			return true
		}
	}
	return false
}
