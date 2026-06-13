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
