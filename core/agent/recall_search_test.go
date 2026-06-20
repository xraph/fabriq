// core/agent/recall_search_test.go
package agent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestRecall_SearchChannelContributesRankedRefs(t *testing.T) {
	reg := testRegistry(t) // tDoc: searchable index "docs"
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, nil, Config{}) // nil embedder isolates the search channel
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "alpha widget", Body: "the body"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Search.ApplyMutations(ctx, "docs", []projection.Mutation{
		projection.DocIndex{Index: "docs", ID: res.AggID, Version: 1, Doc: map[string]any{
			"id": res.AggID, "tenant_id": "acme", "title": "alpha widget", "body": "the body",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	pack, err := tk.Recall(ctx, RecallRequest{Query: "alpha", Budget: 100000, Entities: []string{"doc"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(pack.Items) != 1 || pack.Items[0].ID != res.AggID {
		t.Fatalf("want 1 search hit %q, got %+v (warnings %v)", res.AggID, pack.Items, pack.Warnings)
	}
	found := false
	for _, s := range pack.Items[0].Source {
		if s == "search" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("want search provenance in source, got %v", pack.Items[0].Source)
	}
	// row is the hydrated authoritative relational row (a tDoc), not the index doc
	var row tDoc
	if err := json.Unmarshal(pack.Items[0].Row, &row); err != nil {
		t.Fatal(err)
	}
	if row.Title != "alpha widget" {
		t.Fatalf("want hydrated Title, got %q", row.Title)
	}
}

type tThing struct {
	grove.BaseModel `grove:"table:things"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Name            string `grove:"name,notnull"`
}

func nonSearchableRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{Name: "thing", Kind: registry.KindAggregate, Model: (*tThing)(nil)})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRecall_NonSearchableEntitySkippedSilently(t *testing.T) {
	// An entity with no Search.Index must not error the search channel.
	reg := nonSearchableRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t)

	pack, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100, Entities: []string{"thing"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	// no embedder + not searchable → 0 items, no search-failure warning
	if len(pack.Items) != 0 {
		t.Fatalf("want 0 items, got %d", len(pack.Items))
	}
	for _, wn := range pack.Warnings {
		if strings.HasPrefix(wn, "search") {
			t.Fatalf("non-searchable entity should not warn, got %q", wn)
		}
	}
}
