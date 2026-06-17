package query

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// graphStub is a fake GraphQuerier for graph-cache unit tests.
// Query returns canned ids and counts calls; TraverseAndHydrate and
// ApplyMutations are no-ops (not exercised by the cached paths).
type graphStub struct {
	queries int
	ids     []string
}

func (g *graphStub) Query(_ context.Context, _ string, _ map[string]any, into any) error {
	g.queries++
	*(into.(*[]string)) = append([]string(nil), g.ids...)
	return nil
}

func (g *graphStub) TraverseAndHydrate(_ context.Context, _ string, _ map[string]any, _ any) error {
	return nil
}

func (g *graphStub) ApplyMutations(_ context.Context, _ string, _ []projection.Mutation) error {
	return nil
}

// TestTraverse_CachedIDList: first Traverse queries the graph (g.queries==1);
// a second identical Traverse hits the id-list cache (g.queries STAYS 1).
func TestTraverse_CachedIDList(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"a1": {ID: "a1"}, "a2": {ID: "a2"}}}
	g := &graphStub{ids: []string{"a1", "a2"}}
	fc := newListTestCache()
	repo := newCachedListRepo(t, rel, fc).WithGraph(g)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	if _, err := repo.Traverse(ctx, "MATCH (a:Asset) RETURN a.id", nil); err != nil {
		t.Fatal(err)
	}
	if g.queries != 1 {
		t.Fatalf("first traverse: graph queries=%d (want 1)", g.queries)
	}
	// Second identical Traverse hits the id-list cache: graph NOT queried again.
	if _, err := repo.Traverse(ctx, "MATCH (a:Asset) RETURN a.id", nil); err != nil {
		t.Fatal(err)
	}
	if g.queries != 1 {
		t.Fatalf("second traverse should hit cache: graph queries=%d (want 1)", g.queries)
	}
}

// TestTraverse_NilCache: with no cache wired the non-cached path runs
// TraverseAndHydrate — graph.queries stays 0 because TraverseAndHydrate
// is a no-op on graphStub, confirming the cache branch is not taken.
func TestTraverse_NilCache(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{}}
	g := &graphStub{ids: []string{}}
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: listRow{},
	}); err != nil {
		t.Fatal(err)
	}
	repo, err := For[listRow](reg, rel)
	if err != nil {
		t.Fatal(err)
	}
	repo = repo.WithGraph(g)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	// No cache: must use TraverseAndHydrate (no-op on stub → g.queries stays 0).
	if _, err := repo.Traverse(ctx, "MATCH (a:Asset) RETURN a.id", nil); err != nil {
		t.Fatal(err)
	}
	if g.queries != 0 {
		t.Fatalf("nil-cache Traverse should call TraverseAndHydrate, not Query: g.queries=%d", g.queries)
	}
}

// graphWalkRow is a grove-tagged model with a self-edge for walk cache tests.
type graphWalkRow struct {
	grove.BaseModel `grove:"table:graph_walk_rows"`

	ID       string `json:"id" grove:"id,pk"`
	TenantID string `json:"tenant_id" grove:"tenant_id,notnull"`
	Version  int64  `json:"version" grove:"version,notnull"`
}

// newCachedWalkRepo builds a Repo[graphWalkRow] with cache enabled, a
// GraphNode ("GWR") and a self-edge ("LINKED_TO"). This is needed because
// walk (→ Out/In/Reachable) requires a registry self-edge to pass walkCypher
// validation; the listRow entity used by newCachedListRepo has neither.
func newCachedWalkRepo(t *testing.T, rel RelationalQuerier, c cache.Cache) *Repo[graphWalkRow] {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:      "gwr",
		Kind:      registry.KindAggregate,
		Model:     graphWalkRow{},
		GraphNode: "GWR",
		Edges: []registry.EdgeSpec{
			{Field: "id", Rel: "LINKED_TO", Target: "gwr"},
		},
		Cache: &registry.CacheSpec{TTL: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	repo, err := For[graphWalkRow](reg, rel)
	if err != nil {
		t.Fatal(err)
	}
	ks := cache.Keyspace{
		Name:      "gwr:q",
		Version:   1,
		Entity:    "gwr",
		Partition: cache.Tenant,
		Policy:    cache.Policy{Mode: cache.Versioned, TTL: time.Minute},
	}
	return repo.WithResultCache(c, ks)
}

// relStubWalk is a minimal RelationalQuerier for graphWalkRow.
type relStubWalk struct {
	rows map[string]graphWalkRow
}

func (s *relStubWalk) Get(_ context.Context, _ string, _ string, _ any) error { return nil }
func (s *relStubWalk) GetMany(_ context.Context, _ string, ids []string, into any) error {
	out := into.(*[]*graphWalkRow)
	for _, id := range ids {
		if r, ok := s.rows[id]; ok {
			rc := r
			*out = append(*out, &rc)
		}
	}
	return nil
}
func (s *relStubWalk) List(_ context.Context, _ string, _ ListQuery, _ any) error { return nil }
func (s *relStubWalk) Query(_ context.Context, _ any, _ string, _ ...any) error   { return nil }

// TestWalk_CachedIDList: Out (backed by walk) is called twice with the same
// (id, rel) — first call queries the graph stub, second hits the id-list cache.
func TestWalk_CachedIDList(t *testing.T) {
	rel := &relStubWalk{rows: map[string]graphWalkRow{"b1": {ID: "b1"}, "b2": {ID: "b2"}}}
	g := &graphStub{ids: []string{"b1", "b2"}}
	fc := newListTestCache()
	repo := newCachedWalkRepo(t, rel, fc).WithGraph(g)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	if _, err := repo.Out(ctx, "root", "LINKED_TO"); err != nil {
		t.Fatal(err)
	}
	if g.queries != 1 {
		t.Fatalf("first Out (walk): graph queries=%d (want 1)", g.queries)
	}
	// Second identical Out hits the id-list cache: graph NOT queried again.
	if _, err := repo.Out(ctx, "root", "LINKED_TO"); err != nil {
		t.Fatal(err)
	}
	if g.queries != 1 {
		t.Fatalf("second Out should hit cache: graph queries=%d (want 1)", g.queries)
	}
}
