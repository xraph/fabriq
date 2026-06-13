//go:build integration

package elastic_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

func openES(t *testing.T) (*elastic.Adapter, *registry.Registry) {
	t.Helper()
	url := fabriqtest.StartElasticsearch(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	a, err := elastic.Open(context.Background(), elastic.Config{Addrs: []string{url}}, reg)
	if err != nil {
		t.Fatalf("elastic.Open: %v", err)
	}
	return a, reg
}

func esCtx(t *testing.T, tid string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tid)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func doc(id, name string, version int64) projection.DocIndex {
	return projection.DocIndex{
		Index: "assets", ID: id, Version: version,
		Doc: map[string]any{"id": id, "tenant_id": "acme", "version": version, "name": name},
	}
}

func search(t *testing.T, a *elastic.Adapter, ctx context.Context, q string) []map[string]any {
	t.Helper()
	var hits []map[string]any
	if err := a.Search(ctx, query.SearchQuery{Entity: "asset", Query: q}, &hits); err != nil {
		t.Fatalf("Search(%q): %v", q, err)
	}
	return hits
}

func TestES_IndexSearchVersionGateDeindex(t *testing.T) {
	a, _ := openES(t)
	ctx := esCtx(t, "acme")

	if err := a.ApplyMutations(ctx, "", []projection.Mutation{doc("A1", "Main Pump", 2)}); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}

	hits := search(t, a, ctx, "pump")
	if len(hits) != 1 || hits[0]["id"] != "A1" {
		t.Fatalf("hits = %v", hits)
	}

	// Stale replay (v1) must NOT overwrite v2 — external_gte gate.
	if err := a.ApplyMutations(ctx, "", []projection.Mutation{doc("A1", "STALE NAME", 1)}); err != nil {
		t.Fatalf("stale apply must be swallowed by the version gate: %v", err)
	}
	hits = search(t, a, ctx, "pump")
	if len(hits) != 1 {
		t.Fatalf("doc lost after stale replay: %v", hits)
	}
	if hits[0]["name"] != "Main Pump" {
		t.Fatalf("stale write won: %v", hits[0])
	}

	// Cross-tenant isolation: rival's alias doesn't exist -> empty, no error.
	rival := esCtx(t, "rival")
	if got := search(t, a, rival, "pump"); len(got) != 0 {
		t.Fatalf("tenant leak: %v", got)
	}

	// Deindex at a newer version removes the doc.
	if err := a.ApplyMutations(ctx, "", []projection.Mutation{
		projection.DocDeindex{Index: "assets", ID: "A1", Version: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if got := search(t, a, ctx, "pump"); len(got) != 0 {
		t.Fatalf("doc survived deindex: %v", got)
	}
}

// TestES_StructuredQuery exercises the engine-neutral Filter/Sort/Offset
// translation against a live cluster. It stays on mapping-safe ground: a
// numeric version range filter, a numeric version sort, and from/size
// pagination — none depend on the text/keyword sub-field mapping.
func TestES_StructuredQuery(t *testing.T) {
	a, _ := openES(t)
	ctx := esCtx(t, "acme")

	if err := a.ApplyMutations(ctx, "", []projection.Mutation{
		doc("A1", "Main Pump", 1),
		doc("A2", "Backup Pump", 2),
		doc("A3", "Inlet Valve", 3),
	}); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}

	structured := func(q query.SearchQuery) []map[string]any {
		t.Helper()
		var hits []map[string]any
		if err := a.Search(ctx, q, &hits); err != nil {
			t.Fatalf("Search(%+v): %v", q, err)
		}
		return hits
	}

	// Filter-only (no text): version >= 2, sorted ascending by version.
	hits := structured(query.SearchQuery{
		Entity: "asset",
		Filter: query.Where{query.Gte("version", 2)},
		Sort:   "version",
	})
	if len(hits) != 2 || hits[0]["id"] != "A2" || hits[1]["id"] != "A3" {
		t.Fatalf("filter+sort = %v", hits)
	}

	// Text + filter: pumps with version >= 2 -> only A2.
	hits = structured(query.SearchQuery{
		Entity: "asset", Query: "pump",
		Filter: query.Where{query.Gte("version", 2)},
	})
	if len(hits) != 1 || hits[0]["id"] != "A2" {
		t.Fatalf("text+filter = %v", hits)
	}

	// Pagination: all three by version, skip 1, take 1 -> A2.
	hits = structured(query.SearchQuery{
		Entity: "asset", Sort: "version", Offset: 1, Limit: 1,
	})
	if len(hits) != 1 || hits[0]["id"] != "A2" {
		t.Fatalf("paginated = %v", hits)
	}

	// Validation: a non-indexed column is rejected before any ES call.
	var sink []map[string]any
	if err := a.Search(ctx, query.SearchQuery{Entity: "asset", Filter: query.Where{query.Eq("site_id", "S1")}}, &sink); err == nil {
		t.Fatal("filter on a non-indexed column must be rejected")
	}
}

func TestES_AliasSwapCutover(t *testing.T) {
	a, _ := openES(t)
	ctx := esCtx(t, "acme")

	// Live (v1) content.
	if err := a.ApplyMutations(ctx, "", []projection.Mutation{doc("A1", "Old World", 1)}); err != nil {
		t.Fatal(err)
	}
	if hits := search(t, a, ctx, "old"); len(hits) != 1 {
		t.Fatalf("v1 content missing: %v", hits)
	}

	// Build v2 behind the alias (rebuild target tag).
	if err := a.ApplyMutations(ctx, "v2", []projection.Mutation{doc("A1", "New World", 1)}); err != nil {
		t.Fatal(err)
	}
	// Alias still serves v1 until the flip.
	if hits := search(t, a, ctx, "new"); len(hits) != 0 {
		t.Fatalf("alias moved before flip: %v", hits)
	}

	// Atomic cutover.
	if err := a.FlipAliases(context.Background(), "acme", 1, 2); err != nil {
		t.Fatalf("FlipAliases: %v", err)
	}
	if hits := search(t, a, ctx, "new"); len(hits) != 1 {
		t.Fatalf("alias did not flip to v2: %v", hits)
	}
	if hits := search(t, a, ctx, "old"); len(hits) != 0 {
		t.Fatalf("old content still served after flip: %v", hits)
	}

	// Drop the old version's indexes; the alias keeps serving v2.
	if err := a.DropTarget(ctx, "v1"); err != nil {
		t.Fatalf("DropTarget: %v", err)
	}
	if hits := search(t, a, ctx, "new"); len(hits) != 1 {
		t.Fatalf("serving broke after old-index drop: %v", hits)
	}
}
