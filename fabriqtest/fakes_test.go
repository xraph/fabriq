package fabriqtest_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

type ftSite struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

type ftAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
	SiteID   string `grove:"site_id"`
	ParentID string `grove:"parent_id"`
}

func ftRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "site", Kind: registry.KindAggregate, Model: (*ftSite)(nil), GraphNode: "Site",
		Search:    registry.SearchSpec{Index: "sites", Fields: []string{"name"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	})
	r.MustRegister(registry.EntitySpec{
		Name: "asset", Kind: registry.KindAggregate, Model: (*ftAsset)(nil), GraphNode: "Asset",
		Edges: []registry.EdgeSpec{
			{Field: "site_id", Rel: "LOCATED_AT", Target: "site"},
			{Field: "parent_id", Rel: "CHILD_OF", Target: "asset"},
		},
		Search:    registry.SearchSpec{Index: "assets", Fields: []string{"name"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("site", "site_id"), registry.ByTenant},
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func ftCtx(t testing.TB, tid string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tid)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// --- World: command store + relational share one memory ---------------------

func TestWorld_ExecThenRelationalGet(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := ftCtx(t, "acme")

	res, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &ftSite{Name: "Plant A"}})
	if err != nil {
		t.Fatal(err)
	}

	var got ftSite
	if err := w.Rel.Get(ctx, "site", res.AggID, &got); err != nil {
		t.Fatalf("Rel.Get: %v", err)
	}
	if got.Name != "Plant A" || got.Version != 1 || got.TenantID != "acme" || got.ID != res.AggID {
		t.Fatalf("got %+v", got)
	}
	if len(w.Store.Outbox()) != 1 {
		t.Fatalf("outbox = %d", len(w.Store.Outbox()))
	}
}

func TestWorld_TenantIsolation(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, _ := command.NewExecutor(w.Registry, w.Store)

	res, err := x.Exec(ftCtx(t, "acme"), command.Command{Entity: "site", Op: command.OpCreate, Payload: &ftSite{Name: "Secret"}})
	if err != nil {
		t.Fatal(err)
	}

	var leak ftSite
	err = w.Rel.Get(ftCtx(t, "rival"), "site", res.AggID, &leak)
	if !errors.Is(err, fabriqtest.ErrFakeNotFound) {
		t.Fatalf("cross-tenant read must be not-found, got %v (leak=%+v)", err, leak)
	}
}

func TestFakeRelational_GetManyPreservesIDOrder(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, _ := command.NewExecutor(w.Registry, w.Store)
	ctx := ftCtx(t, "acme")

	ids := make([]string, 3)
	for i, name := range []string{"A", "B", "C"} {
		res, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &ftSite{Name: name}})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = res.AggID
	}

	var got []*ftSite
	if err := w.Rel.GetMany(ctx, "site", []string{ids[2], ids[0], "missing"}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != ids[2] || got[1].ID != ids[0] {
		t.Fatalf("GetMany order/skip wrong: %+v", got)
	}
}

func TestFakeRelational_ListFilterAndLimit(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, _ := command.NewExecutor(w.Registry, w.Store)
	ctx := ftCtx(t, "acme")

	site, _ := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &ftSite{Name: "S"}})
	for _, name := range []string{"P1", "P2", "P3"} {
		if _, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &ftAsset{Name: name, SiteID: site.AggID}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &ftAsset{Name: "elsewhere"}}); err != nil {
		t.Fatal(err)
	}

	var got []*ftAsset
	if err := w.Rel.List(ctx, "asset", query.ListQuery{
		Filter: map[string]any{"site_id": site.AggID}, Limit: 2,
	}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("List = %d rows, want 2 (limit)", len(got))
	}
	for _, a := range got {
		if a.SiteID != site.AggID {
			t.Fatalf("filter leak: %+v", a)
		}
	}
}

// --- FakeGraph ---------------------------------------------------------------

func TestFakeGraph_MutationsAndVersionGating(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")
	target := registry.GraphName("acme")

	muts := []projection.Mutation{
		projection.NodeUpsert{Label: "Asset", ID: "A1", Props: map[string]any{"name": "Pump", "version": 2}, Version: 2},
		projection.EdgeUpsert{Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A1", ToLabel: "Site", ToID: "S1", Version: 2},
	}
	if err := w.Graph.ApplyMutations(ctx, target, muts); err != nil {
		t.Fatal(err)
	}

	// Stale mutation (v1) must be ignored — idempotency gate.
	stale := []projection.Mutation{
		projection.NodeUpsert{Label: "Asset", ID: "A1", Props: map[string]any{"name": "OLD", "version": 1}, Version: 1},
	}
	if err := w.Graph.ApplyMutations(ctx, target, stale); err != nil {
		t.Fatal(err)
	}

	node, ok := w.Graph.Node(target, "Asset", "A1")
	if !ok || node.Props["name"] != "Pump" {
		t.Fatalf("stale write won: %+v", node)
	}
	if !w.Graph.HasEdge(target, "LOCATED_AT", "Asset", "A1", "Site", "S1") {
		t.Fatal("edge missing")
	}

	// Delete detaches.
	if err := w.Graph.ApplyMutations(ctx, target, []projection.Mutation{
		projection.NodeDelete{Label: "Asset", ID: "A1"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.Graph.Node(target, "Asset", "A1"); ok {
		t.Fatal("node survived delete")
	}
	if w.Graph.HasEdge(target, "LOCATED_AT", "Asset", "A1", "Site", "S1") {
		t.Fatal("edge survived detach-delete")
	}
}

func TestFakeGraph_TraverseAndHydrate(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, _ := command.NewExecutor(w.Registry, w.Store)
	ctx := ftCtx(t, "acme")

	a1, _ := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: "P1"}})
	a2, _ := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: "P2"}})

	cypher := "MATCH (a:Asset)-[:LOCATED_AT]->(s:Site {id:$site}) RETURN a.id"
	w.Graph.Cann(cypher, []string{a1.AggID, a2.AggID})

	var out []*ftAsset
	if err := w.Graph.TraverseAndHydrate(ctx, cypher, map[string]any{"site": "S1"}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].Name != "P1" || out[1].Name != "P2" {
		t.Fatalf("hydrated = %+v", out)
	}
}

// --- FakeSearch ----------------------------------------------------------------

func TestFakeSearch_IndexSearchDeindex(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")
	alias := registry.SearchIndexAlias("acme", "assets")

	if err := w.Search.ApplyMutations(ctx, alias, []projection.Mutation{
		projection.DocIndex{Index: "assets", ID: "A1", Doc: map[string]any{"id": "A1", "tenant_id": "acme", "name": "Main Pump"}, Version: 1},
		projection.DocIndex{Index: "assets", ID: "A2", Doc: map[string]any{"id": "A2", "tenant_id": "acme", "name": "Backup Valve"}, Version: 1},
	}); err != nil {
		t.Fatal(err)
	}

	var hits []map[string]any
	if err := w.Search.Search(ctx, query.SearchQuery{Entity: "asset", Query: "pump"}, &hits); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0]["id"] != "A1" {
		t.Fatalf("hits = %v", hits)
	}

	// Cross-tenant search must see nothing.
	var rivalHits []map[string]any
	if err := w.Search.Search(ftCtx(t, "rival"), query.SearchQuery{Entity: "asset", Query: "pump"}, &rivalHits); err != nil {
		t.Fatal(err)
	}
	if len(rivalHits) != 0 {
		t.Fatalf("tenant leak: %v", rivalHits)
	}

	if err := w.Search.ApplyMutations(ctx, alias, []projection.Mutation{
		projection.DocDeindex{Index: "assets", ID: "A1"},
	}); err != nil {
		t.Fatal(err)
	}
	hits = nil
	if err := w.Search.Search(ctx, query.SearchQuery{Entity: "asset", Query: "pump"}, &hits); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("deindexed doc still found: %v", hits)
	}
}

// --- FakeTS -------------------------------------------------------------------

func TestFakeTS_BulkWriteAndRange(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")
	base := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)

	points := make([]query.Point, 10)
	for i := range points {
		points[i] = query.Point{Key: "tag-1", At: base.Add(time.Duration(i) * time.Minute), Value: float64(i)}
	}
	if err := w.TS.BulkWrite(ctx, "tag_readings", points); err != nil {
		t.Fatal(err)
	}

	var got []query.Point
	if err := w.TS.Range(ctx, query.RangeQuery{
		Series: "tag_readings", Key: "tag-1",
		From: base.Add(2 * time.Minute), To: base.Add(5 * time.Minute),
	}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 { // [from, to): minutes 2,3,4
		t.Fatalf("Range = %d points, want 3: %+v", len(got), got)
	}
}

// --- FakeVector ------------------------------------------------------------------

func TestFakeVector_SimilarOrdersByCosine(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")

	if err := w.Vector.Upsert(ctx, "asset", "A1", []float32{1, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "asset", "A2", []float32{0, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "asset", "A3", []float32{0.9, 0.1}, nil); err != nil {
		t.Fatal(err)
	}

	var got []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "asset", Embedding: []float32{1, 0}, K: 2}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "A1" || got[1].ID != "A3" {
		t.Fatalf("Similar = %+v", got)
	}
}

// --- FakeDocumentStore -------------------------------------------------------------

func TestFakeDocumentStore_DeferredPlane(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	ctx := ftCtx(t, "acme")
	if err := w.Docs.ApplyUpdate(ctx, "D1", []byte{1}); err == nil || !strings.Contains(err.Error(), "document plane") {
		t.Fatalf("deferred plane must say so, got %v", err)
	}
}

// --- FakeStore failure injection (used by batch-atomicity tests downstream) ---

func TestFakeStore_OutboxFailureInjection(t *testing.T) {
	w := fabriqtest.NewWorld(ftRegistry(t))
	w.Store.FailOnOutbox(func() error { return errors.New("boom") })
	x, _ := command.NewExecutor(w.Registry, w.Store)

	_, err := x.Exec(ftCtx(t, "acme"), command.Command{Entity: "site", Op: command.OpCreate, Payload: &ftSite{Name: "A"}})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want injected failure, got %v", err)
	}
	var ghost ftSite
	if err := w.Rel.Get(ftCtx(t, "acme"), "site", "anything", &ghost); !errors.Is(err, fabriqtest.ErrFakeNotFound) {
		t.Fatal("failed tx must not leave rows behind")
	}
}
