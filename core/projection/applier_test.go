package projection_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
)

type benchSite struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

type benchAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
	Serial   string `grove:"serial"`
	SiteID   string `grove:"site_id"`
	ParentID string `grove:"parent_id"`
}

func testRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "site", Kind: registry.KindAggregate, Model: (*benchSite)(nil),
		GraphNode: "Site",
		Search:    registry.SearchSpec{Index: "sites", Fields: []string{"name"}},
	})
	r.MustRegister(registry.EntitySpec{
		Name: "asset", Kind: registry.KindAggregate, Model: (*benchAsset)(nil),
		GraphNode: "Asset",
		Edges: []registry.EdgeSpec{
			{Field: "site_id", Rel: "LOCATED_AT", Target: "site"},
			{Field: "parent_id", Rel: "CHILD_OF", Target: "asset"},
		},
		Search: registry.SearchSpec{Index: "assets", Fields: []string{"name", "serial"}},
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func assetEnvelope(t testing.TB, verb string, payload map[string]any) event.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return event.Envelope{
		ID: event.NewID(), TenantID: "acme", Aggregate: "asset", AggID: "A1",
		Version: 7, Type: registry.EventType("asset", verb), At: time.Now().UTC(),
		PayloadSchemaVersion: 1, Payload: raw,
	}
}

func TestGraphApplier_CreateWithBothEdges(t *testing.T) {
	ap := projection.GraphApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbCreated, map[string]any{
		"id": "A1", "tenant_id": "acme", "version": 7, "name": "Pump", "serial": "SN-1",
		"site_id": "S1", "parent_id": "P1",
	})

	muts, err := ap.Apply(env)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(muts) != 3 {
		t.Fatalf("got %d mutations, want 3 (node + 2 edges): %#v", len(muts), muts)
	}

	node, ok := muts[0].(projection.NodeUpsert)
	if !ok {
		t.Fatalf("muts[0] = %T, want NodeUpsert", muts[0])
	}
	if node.Label != "Asset" || node.ID != "A1" || node.Version != 7 {
		t.Fatalf("node = %+v", node)
	}
	if node.Props["name"] != "Pump" || node.Props["version"] != float64(7) {
		t.Fatalf("node props missing data: %v", node.Props)
	}

	wantEdges := map[string]projection.EdgeUpsert{
		"LOCATED_AT": {Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A1", ToLabel: "Site", ToID: "S1", Version: 7},
		"CHILD_OF":   {Rel: "CHILD_OF", FromLabel: "Asset", FromID: "A1", ToLabel: "Asset", ToID: "P1", Version: 7},
	}
	for _, m := range muts[1:] {
		e, ok := m.(projection.EdgeUpsert)
		if !ok {
			t.Fatalf("edge mutation = %T, want EdgeUpsert", m)
		}
		want := wantEdges[e.Rel]
		if e != want {
			t.Fatalf("edge %s = %+v, want %+v", e.Rel, e, want)
		}
		delete(wantEdges, e.Rel)
	}
	if len(wantEdges) != 0 {
		t.Fatalf("missing edges: %v", wantEdges)
	}
}

func TestGraphApplier_UpdateClearingFKDeletesEdge(t *testing.T) {
	ap := projection.GraphApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbUpdated, map[string]any{
		"id": "A1", "tenant_id": "acme", "version": 7, "name": "Pump",
		"site_id": "S1", "parent_id": "", // detached from parent
	})

	muts, err := ap.Apply(env)
	if err != nil {
		t.Fatal(err)
	}
	var sawDelete bool
	for _, m := range muts {
		if d, ok := m.(projection.EdgeDelete); ok {
			if d.Rel != "CHILD_OF" || d.FromID != "A1" {
				t.Fatalf("unexpected EdgeDelete: %+v", d)
			}
			if d.Version != 7 {
				t.Fatalf("EdgeDelete must carry the event version for replay gating, got %d", d.Version)
			}
			sawDelete = true
		}
	}
	if !sawDelete {
		t.Fatal("cleared FK must produce EdgeDelete")
	}
}

func TestGraphApplier_Delete(t *testing.T) {
	ap := projection.GraphApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbDeleted, map[string]any{})

	muts, err := ap.Apply(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts) != 1 {
		t.Fatalf("got %d mutations, want 1", len(muts))
	}
	del, ok := muts[0].(projection.NodeDelete)
	if !ok || del.Label != "Asset" || del.ID != "A1" {
		t.Fatalf("muts[0] = %#v, want NodeDelete{Asset,A1}", muts[0])
	}
}

func TestGraphApplier_UnknownAggregateIsNoOp(t *testing.T) {
	ap := projection.GraphApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbCreated, map[string]any{"id": "A1"})
	env.Aggregate = "unknown_thing"
	env.Type = "unknown_thing.created"

	muts, err := ap.Apply(env)
	if err != nil || len(muts) != 0 {
		t.Fatalf("unknown aggregate: muts=%v err=%v, want none", muts, err)
	}
}

func TestSearchApplier_IndexesOnlyDeclaredFields(t *testing.T) {
	ap := projection.SearchApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbUpdated, map[string]any{
		"id": "A1", "tenant_id": "acme", "version": 7,
		"name": "Pump", "serial": "SN-1", "site_id": "S1",
	})

	muts, err := ap.Apply(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts) != 1 {
		t.Fatalf("got %d mutations, want 1", len(muts))
	}
	doc, ok := muts[0].(projection.DocIndex)
	if !ok {
		t.Fatalf("muts[0] = %T, want DocIndex", muts[0])
	}
	if doc.Index != "assets" || doc.ID != "A1" || doc.Version != 7 {
		t.Fatalf("doc = %+v", doc)
	}
	if doc.Doc["name"] != "Pump" || doc.Doc["serial"] != "SN-1" {
		t.Fatalf("doc fields missing: %v", doc.Doc)
	}
	if _, leaked := doc.Doc["site_id"]; leaked {
		t.Fatal("undeclared field leaked into search doc")
	}
	if doc.Doc["id"] != "A1" || doc.Doc["tenant_id"] != "acme" {
		t.Fatalf("structural fields must always be present: %v", doc.Doc)
	}
}

func TestSearchApplier_DeleteDeindexes(t *testing.T) {
	ap := projection.SearchApplier(testRegistry(t))
	env := assetEnvelope(t, registry.VerbDeleted, map[string]any{})

	muts, err := ap.Apply(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(muts) != 1 {
		t.Fatalf("got %d mutations, want 1", len(muts))
	}
	de, ok := muts[0].(projection.DocDeindex)
	if !ok || de.Index != "assets" || de.ID != "A1" {
		t.Fatalf("muts[0] = %#v, want DocDeindex{assets,A1}", muts[0])
	}
}

func TestSearchApplier_EntityWithoutIndexIsNoOp(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "site", Kind: registry.KindAggregate, Model: (*benchSite)(nil), GraphNode: "Site",
	})
	ap := projection.SearchApplier(r)
	env := assetEnvelope(t, registry.VerbCreated, map[string]any{"id": "S1"})
	env.Aggregate = "site"
	env.Type = "site.created"
	env.AggID = "S1"

	muts, err := ap.Apply(env)
	if err != nil || len(muts) != 0 {
		t.Fatalf("unindexed entity: muts=%v err=%v, want none", muts, err)
	}
}

func BenchmarkGraphApplier(b *testing.B) {
	ap := projection.GraphApplier(testRegistry(b))
	env := assetEnvelope(b, registry.VerbUpdated, map[string]any{
		"id": "A1", "tenant_id": "acme", "version": 7, "name": "Pump",
		"site_id": "S1", "parent_id": "P1",
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ap.Apply(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearchApplier(b *testing.B) {
	ap := projection.SearchApplier(testRegistry(b))
	env := assetEnvelope(b, registry.VerbUpdated, map[string]any{
		"id": "A1", "tenant_id": "acme", "version": 7, "name": "Pump", "serial": "SN-1",
	})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ap.Apply(env); err != nil {
			b.Fatal(err)
		}
	}
}
