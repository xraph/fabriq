package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

func TestRecallFiltersAuthoritativeRows(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		name := "typed"
		if dynamic {
			name = "dynamic"
		}
		t.Run(name, func(t *testing.T) {
			reg := testRegistry(t)
			if dynamic {
				reg = registry.New()
				reg.MustRegister(registry.EntitySpec{Name: "doc", Kind: registry.KindAggregate, Schema: &registry.DynamicSchema{Table: "docs", Columns: []registry.DynamicColumn{{Name: "title", Type: registry.ColText}, {Name: "body", Type: registry.ColText}}}, Search: registry.SearchSpec{Index: "docs", Fields: []string{"title", "body"}}})
				if err := reg.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			w := fabriqtest.NewWorld(reg)
			ff := newFakeFabric(t, w)
			tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{Strict: true, VectorDims: 3})
			if err != nil {
				t.Fatal(err)
			}
			ctx := testCtx(t)
			var keep string
			for _, state := range []string{"allowed", "revoked", "deleted", "foreign", "undiscovered"} {
				rowCtx := ctx
				if state == "foreign" {
					rowCtx, err = tenant.WithTenant(ctx, "other")
					if err != nil {
						t.Fatal(err)
					}
				}
				body := "allowed"
				if state == "revoked" {
					body = "revoked"
				}
				var payload any = &tDoc{Title: "alpha", Body: body}
				if dynamic {
					payload = map[string]any{"title": "alpha", "body": body}
				}
				res, createErr := ff.Exec(rowCtx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: payload})
				if createErr != nil {
					t.Fatal(createErr)
				}
				if state == "allowed" {
					keep = res.AggID
				}
				if state == "deleted" {
					if _, err = ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpDelete, AggID: res.AggID}); err != nil {
						t.Fatal(err)
					}
				}
				if state == "undiscovered" {
					continue
				}
				if err = w.Vector.Upsert(ctx, "doc", res.AggID, []float32{1, 0, 0}, nil); err != nil {
					t.Fatal(err)
				}
				if err = w.Search.ApplyMutations(ctx, "docs", []projection.Mutation{projection.DocIndex{Index: "docs", ID: res.AggID, Version: 1, Doc: map[string]any{"id": res.AggID, "tenant_id": "acme", "title": "alpha", "body": "allowed"}}}); err != nil {
					t.Fatal(err)
				}
			}
			filters := query.Where{query.Or(query.Eq("body", "allowed"), query.Eq("title", "special"))}
			pack, err := tk.Recall(ctx, RecallRequest{Query: "alpha", Budget: 10000, K: 10, Entities: []string{"doc"}, Filters: filters})
			if err != nil {
				t.Fatal(err)
			}
			if len(pack.Items) != 1 || pack.Items[0].ID != keep {
				t.Fatalf("want only current allowed row %s, got %+v", keep, pack.Items)
			}
			if len(filters) != 1 || len(filters[0].Or) != 2 {
				t.Fatal("recall mutated caller filter")
			}
		})
	}
}

func TestRecallFilteredEntitiesDoNotIncludeAutomaticDigests(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	ctx := testCtx(t)
	if _, err := fab.Exec(ctx, command.Command{Entity: "note", Op: command.OpCreate, AggID: "n1", Payload: &tDoc{Title: "Pump", Body: "allowed"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "note", "n1", []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, &fakeSummarizer{}, nil, fabriqtest.NewFakeCAS(), DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "Pump", "body": "allowed"}); err != nil {
		t.Fatal(err)
	}
	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{Strict: true, VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	pack, err := tk.Recall(ctx, RecallRequest{Query: "pump", Budget: 10000, Entities: []string{"note"}, Filters: query.Where{query.Eq("body", "allowed")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pack.Items) != 1 || pack.Items[0].ID != "n1" || pack.Items[0].Entity != "note" {
		t.Fatalf("unexpected filtered pack: %+v", pack)
	}
}
