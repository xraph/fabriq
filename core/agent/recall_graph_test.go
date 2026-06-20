// core/agent/recall_graph_test.go
package agent

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

type gAsset struct {
	grove.BaseModel `grove:"table:assets"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Name            string `grove:"name,notnull"`
	SiteID          string `grove:"site_id"`
}

type gSite struct {
	grove.BaseModel `grove:"table:sites"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Name            string `grove:"name,notnull"`
}

func graphRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{Name: "gsite", Kind: registry.KindAggregate, Model: (*gSite)(nil), GraphNode: "GSite"})
	r.MustRegister(registry.EntitySpec{
		Name: "gasset", Kind: registry.KindAggregate, Model: (*gAsset)(nil), GraphNode: "GAsset",
		Edges: []registry.EdgeSpec{{Field: "site_id", Rel: "LOCATED_AT", Target: "gsite"}},
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRecall_GraphChannelExpandsToNeighbor(t *testing.T) {
	reg := graphRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	site, err := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "Plant A"}})
	if err != nil {
		t.Fatal(err)
	}
	asset, err := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "Pump", SiteID: site.AggID}})
	if err != nil {
		t.Fatal(err)
	}
	// asset is the vector seed
	if err = w.Vector.Upsert(ctx, "gasset", asset.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	// graph: gasset --LOCATED_AT--> gsite (canned to the EXACT cypher the channel builds)
	w.Graph.Cann(expansionCypher("GAsset", "LOCATED_AT", "GSite", 1, false), []string{site.AggID})

	pack, err := tk.Recall(ctx, RecallRequest{Query: "pump", Budget: 100000, Entities: []string{"gasset"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}

	var siteItem *ContextItem
	for i := range pack.Items {
		if pack.Items[i].Entity == "gsite" && pack.Items[i].ID == site.AggID {
			siteItem = &pack.Items[i]
		}
	}
	if siteItem == nil {
		t.Fatalf("expected gsite %q via graph expansion; items=%+v warnings=%v", site.AggID, pack.Items, pack.Warnings)
	}
	if len(siteItem.Source) == 0 || siteItem.Source[0] != "graph" {
		t.Fatalf("want graph provenance on gsite, got %v", siteItem.Source)
	}
}

func TestRecall_GraphChannelStrictReturnsError(t *testing.T) {
	reg := graphRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{Strict: true, VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	site, err := ff.Exec(ctx, command.Command{Entity: "gsite", Op: command.OpCreate, Payload: &gSite{Name: "Plant B"}})
	if err != nil {
		t.Fatal(err)
	}
	asset, err := ff.Exec(ctx, command.Command{Entity: "gasset", Op: command.OpCreate, Payload: &gAsset{Name: "Pump 2", SiteID: site.AggID}})
	if err != nil {
		t.Fatal(err)
	}
	// asset is the vector seed — no Graph.Cann, so FakeGraph returns an error
	if err = w.Vector.Upsert(ctx, "gasset", asset.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	_, err = tk.Recall(ctx, RecallRequest{Query: "pump", Budget: 100000, Entities: []string{"gasset"}})
	if err == nil {
		t.Fatal("strict graph channel: expected error from uncanned cypher, got nil")
	}
}

func TestExpansionCypher_Format(t *testing.T) {
	got := expansionCypher("GAsset", "LOCATED_AT", "GSite", 1, false)
	want := "MATCH (n:GAsset {id: $id})-[:LOCATED_AT]->(m:GSite) RETURN m.id"
	if got != want {
		t.Fatalf("cypher mismatch:\n got %q\nwant %q", got, want)
	}
}
