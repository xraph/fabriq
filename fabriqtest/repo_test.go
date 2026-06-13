package fabriqtest_test

import (
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// The typed Repo derives the entity from the grove model type T and
// returns *T / []*T — no entity string, no `any` at the call site.

func repoWorld(t *testing.T) (*fabriqtest.World, *command.Executor) {
	t.Helper()
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		t.Fatal(err)
	}
	return w, x
}

func TestRepo_TypedGetGetManyList(t *testing.T) {
	w, x := repoWorld(t)
	ctx := ftCtx(t, "acme")

	ids := make([]string, 3)
	for i, name := range []string{"Main Pump", "Backup Pump", "Inlet Valve"} {
		res, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: name, SiteID: "S1"}})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = res.AggID
	}

	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatalf("For[ftAsset]: %v", err)
	}

	// Get -> *ftAsset (typed, no cast).
	a, err := repo.Get(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != "Main Pump" { // a is *ftAsset; .Name is compile-checked
		t.Fatalf("Get = %+v", a)
	}

	// GetMany -> []*ftAsset, ids order preserved, missing skipped.
	many, err := repo.GetMany(ctx, []string{ids[2], ids[0], "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(many) != 2 || many[0].ID != ids[2] || many[1].ID != ids[0] {
		t.Fatalf("GetMany = %+v", many)
	}

	// List -> []*ftAsset with the structured filter.
	list, err := repo.List(ctx, query.ListQuery{Where: []query.Cond{query.Like("name", "%Pump")}, OrderBy: "name"})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "Backup Pump" || list[1].Name != "Main Pump" {
		t.Fatalf("List = %+v", list)
	}
}

func TestRepo_One(t *testing.T) {
	w, x := repoWorld(t)
	ctx := ftCtx(t, "acme")

	for _, name := range []string{"Alpha", "Beta", "Beta"} {
		if _, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: name, SiteID: "S1"}}); err != nil {
			t.Fatal(err)
		}
	}
	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatal(err)
	}

	// Exactly one match -> *ftAsset.
	got, err := repo.One(ctx, query.Eq("name", "Alpha"))
	if err != nil || got.Name != "Alpha" {
		t.Fatalf("One(Alpha) = (%+v, %v)", got, err)
	}

	// No match -> ErrNotFound.
	if _, err := repo.One(ctx, query.Eq("name", "Nope")); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("One(none): want ErrNotFound, got %v", err)
	}

	// Multiple matches -> error (One means one).
	if _, err := repo.One(ctx, query.Eq("name", "Beta")); err == nil || errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("One(multiple): want a 'matched multiple' error, got %v", err)
	}
}

func TestRepo_UnregisteredModelFails(t *testing.T) {
	w, _ := repoWorld(t)
	type Stranger struct{ ID string }
	if _, err := query.For[Stranger](w.Registry, w.Rel); err == nil {
		t.Fatal("For on an unregistered model type must fail")
	}
}

func TestRepo_TypedTraverseSearchSimilar(t *testing.T) {
	w, x := repoWorld(t)
	ctx := ftCtx(t, "acme")

	a1, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: "Main Pump", SiteID: "S1"}})
	if err != nil {
		t.Fatal(err)
	}
	a2, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: "Inlet Valve", SiteID: "S1"}})
	if err != nil {
		t.Fatal(err)
	}

	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatal(err)
	}
	repo = repo.WithGraph(w.Graph).WithSearch(w.Search).WithVector(w.Vector)

	// Traverse: graph returns ids -> hydrated []*ftAsset in that order.
	cypher := "MATCH (a:Asset)-[:LOCATED_AT]->(:Site {id:$s}) RETURN a.id"
	w.Graph.Cann(cypher, []string{a2.AggID, a1.AggID})
	traversed, err := repo.Traverse(ctx, cypher, map[string]any{"s": "S1"})
	if err != nil {
		t.Fatalf("Traverse: %v", err)
	}
	if len(traversed) != 2 || traversed[0].ID != a2.AggID || traversed[1].Name != "Main Pump" {
		t.Fatalf("Traverse = %+v", traversed)
	}

	// Search: index docs, search -> hydrated []*ftAsset.
	if err = w.Search.ApplyMutations(ctx, "assets", []projection.Mutation{
		projection.DocIndex{Index: "assets", ID: a1.AggID, Doc: map[string]any{"id": a1.AggID, "tenant_id": "acme", "name": "Main Pump"}, Version: 1},
	}); err != nil {
		t.Fatal(err)
	}
	found, err := repo.Search(ctx, "pump", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(found) != 1 || found[0].ID != a1.AggID || found[0].Name != "Main Pump" {
		t.Fatalf("Search = %+v", found)
	}

	// Similar: upsert vectors, similarity -> hydrated []*ftAsset.
	if err = w.Vector.Upsert(ctx, "asset", a1.AggID, []float32{1, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err = w.Vector.Upsert(ctx, "asset", a2.AggID, []float32{0, 1}, nil); err != nil {
		t.Fatal(err)
	}
	near, err := repo.Similar(ctx, []float32{1, 0}, 1)
	if err != nil {
		t.Fatalf("Similar: %v", err)
	}
	if len(near) != 1 || near[0].ID != a1.AggID {
		t.Fatalf("Similar = %+v", near)
	}
}

// The bounded traversal helpers emit a fixed common-subset Cypher string;
// these are pinned here (the fake graph matches Cypher exactly) so any
// change to the emitted query is a conscious, conformance-reviewed edit.
func TestRepo_TypedGraphWalk(t *testing.T) {
	w, x := repoWorld(t)
	ctx := ftCtx(t, "acme")

	// A small CHILD_OF hierarchy: g(rand) <- p(arent) <- c(hild).
	ids := make(map[string]string)
	for _, name := range []string{"grand", "parent", "child"} {
		res, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: name, SiteID: "S1"}})
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = res.AggID
	}

	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatal(err)
	}
	repo = repo.WithGraph(w.Graph)

	// Out: child -[:CHILD_OF]-> parent.
	w.Graph.Cann("MATCH (n:Asset {id: $id})-[:CHILD_OF]->(m:Asset) RETURN m.id ORDER BY m.id", []string{ids["parent"]})
	parents, err := repo.Out(ctx, ids["child"], "CHILD_OF")
	if err != nil {
		t.Fatalf("Out: %v", err)
	}
	if len(parents) != 1 || parents[0].Name != "parent" {
		t.Fatalf("Out = %+v", parents)
	}

	// In: parent <-[:CHILD_OF]- child.
	w.Graph.Cann("MATCH (n:Asset {id: $id})<-[:CHILD_OF]-(m:Asset) RETURN m.id ORDER BY m.id", []string{ids["child"]})
	children, err := repo.In(ctx, ids["parent"], "CHILD_OF")
	if err != nil {
		t.Fatalf("In: %v", err)
	}
	if len(children) != 1 || children[0].Name != "child" {
		t.Fatalf("In = %+v", children)
	}

	// Reachable: ancestors of child within 1..3 hops — duplicate ids from
	// multiple paths must be deduped before hydration.
	w.Graph.Cann("MATCH (n:Asset {id: $id})-[:CHILD_OF*1..3]->(m:Asset) RETURN m.id ORDER BY m.id",
		[]string{ids["parent"], ids["grand"], ids["grand"]})
	ancestors, err := repo.Reachable(ctx, ids["child"], "CHILD_OF", 1, 3)
	if err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	if len(ancestors) != 2 || ancestors[0].Name != "parent" || ancestors[1].Name != "grand" {
		t.Fatalf("Reachable = %+v", ancestors)
	}
}

func TestRepo_GraphWalkValidation(t *testing.T) {
	w, _ := repoWorld(t)
	ctx := ftCtx(t, "acme")
	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatal(err)
	}

	// Graph not attached -> ErrStoreNotConfigured.
	if _, err := repo.Out(ctx, "x", "CHILD_OF"); !errors.Is(err, fabriqerr.ErrStoreNotConfigured) {
		t.Fatalf("Out without graph: want ErrStoreNotConfigured, got %v", err)
	}

	repo = repo.WithGraph(w.Graph)

	// Cross-type edge (LOCATED_AT -> site) is not a self-edge: rejected,
	// not silently empty.
	if _, err := repo.Out(ctx, "x", "LOCATED_AT"); err == nil || errors.Is(err, fabriqerr.ErrStoreNotConfigured) {
		t.Fatalf("Out(LOCATED_AT): want a 'not a self-edge' error, got %v", err)
	}
	// Undeclared / injection-y relationship type is rejected before any query.
	if _, err := repo.Out(ctx, "x", "CHILD_OF]->() DETACH DELETE n //"); err == nil {
		t.Fatal("Out with an invalid relationship identifier must fail")
	}
	// Bad hop ranges.
	if _, err := repo.Reachable(ctx, "x", "CHILD_OF", 0, 3); err == nil {
		t.Fatal("Reachable with minHops < 1 must fail")
	}
	if _, err := repo.Reachable(ctx, "x", "CHILD_OF", 3, 1); err == nil {
		t.Fatal("Reachable with max < min must fail")
	}
	if _, err := repo.Reachable(ctx, "x", "CHILD_OF", 1, 999); err == nil {
		t.Fatal("Reachable beyond the hop cap must fail")
	}
}

func TestRepo_SearchWith(t *testing.T) {
	w, x := repoWorld(t)
	ctx := ftCtx(t, "acme")

	// Seed relational rows + index a search doc per row (id, name, version).
	type seed struct {
		name    string
		version int64
	}
	ids := make(map[string]string)
	for _, s := range []seed{{"Main Pump", 1}, {"Backup Pump", 2}, {"Inlet Valve", 3}} {
		res, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate, Payload: &ftAsset{Name: s.name, SiteID: "S1"}})
		if err != nil {
			t.Fatal(err)
		}
		ids[s.name] = res.AggID
		if err := w.Search.ApplyMutations(ctx, "assets", []projection.Mutation{
			projection.DocIndex{Index: "assets", ID: res.AggID, Version: s.version,
				Doc: map[string]any{"id": res.AggID, "tenant_id": "acme", "name": s.name, "version": s.version}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	repo, err := query.For[ftAsset](w.Registry, w.Rel)
	if err != nil {
		t.Fatal(err)
	}
	repo = repo.WithSearch(w.Search)

	// Free text + sort by name: both pumps, ascending.
	pumps, err := repo.SearchWith(ctx, query.SearchRequest{Query: "pump", Sort: "name"})
	if err != nil {
		t.Fatalf("SearchWith text+sort: %v", err)
	}
	if len(pumps) != 2 || pumps[0].Name != "Backup Pump" || pumps[1].Name != "Main Pump" {
		t.Fatalf("text+sort = %+v", pumps)
	}

	// No text + structured filter (version >= 2) + numeric sort: all docs
	// matched, then narrowed and ordered by version.
	recent, err := repo.SearchWith(ctx, query.SearchRequest{
		Filter: query.Where{query.Gte("version", 2)},
		Sort:   "version",
	})
	if err != nil {
		t.Fatalf("SearchWith filter: %v", err)
	}
	if len(recent) != 2 || recent[0].Name != "Backup Pump" || recent[1].Name != "Inlet Valve" {
		t.Fatalf("filter+sort = %+v", recent)
	}

	// Pagination: sort by name, skip 1, take 1 -> the middle row.
	page, err := repo.SearchWith(ctx, query.SearchRequest{Sort: "name", Offset: 1, Limit: 1})
	if err != nil {
		t.Fatalf("SearchWith page: %v", err)
	}
	if len(page) != 1 || page[0].Name != "Inlet Valve" {
		t.Fatalf("page = %+v", page)
	}

	// Filter/sort may only reference INDEXED fields. site_id is a column
	// but not in the search index -> rejected, both as a filter and a sort.
	if _, err := repo.SearchWith(ctx, query.SearchRequest{Filter: query.Where{query.Eq("site_id", "S1")}}); err == nil {
		t.Fatal("filter on a non-indexed field must be rejected")
	}
	if _, err := repo.SearchWith(ctx, query.SearchRequest{Sort: "site_id"}); err == nil {
		t.Fatal("sort on a non-indexed field must be rejected")
	}
}

func TestRepo_CapabilityNotConfigured(t *testing.T) {
	w, _ := repoWorld(t)
	ctx := ftCtx(t, "acme")
	repo, err := query.For[ftAsset](w.Registry, w.Rel) // relational only
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Traverse(ctx, "MATCH (a) RETURN a.id", nil); !errors.Is(err, fabriqerr.ErrStoreNotConfigured) {
		t.Fatalf("Traverse without graph: want ErrStoreNotConfigured, got %v", err)
	}
	if _, err := repo.Search(ctx, "x", 1); !errors.Is(err, fabriqerr.ErrStoreNotConfigured) {
		t.Fatalf("Search without search: want ErrStoreNotConfigured, got %v", err)
	}
	if _, err := repo.Similar(ctx, []float32{1}, 1); !errors.Is(err, fabriqerr.ErrStoreNotConfigured) {
		t.Fatalf("Similar without vector: want ErrStoreNotConfigured, got %v", err)
	}
}
