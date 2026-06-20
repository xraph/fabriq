// core/agent/hydrate_test.go
package agent

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestHydrate_TypedEntity(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, err := NewToolkit(ff, reg, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "Hello", Body: "World"}})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := tk.hydrate(ctx, "doc", []string{res.AggID})
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	raw, ok := rows[res.AggID]
	if !ok {
		t.Fatalf("missing id %q; got %v", res.AggID, rows)
	}
	var got tDoc
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Title != "Hello" {
		t.Fatalf("want Title Hello, got %q", got.Title)
	}
}

func TestHydrate_DynamicEntity(t *testing.T) {
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "orders", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
			},
		},
	})
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, err := NewToolkit(ff, reg, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	res, err := ff.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpCreate,
		Payload: map[string]any{"sku": "A1", "qty": 3},
	})
	if err != nil {
		t.Fatalf("seed dynamic entity: %v", err)
	}

	rows, err := tk.hydrate(ctx, "orders", []string{res.AggID})
	if err != nil {
		t.Fatalf("hydrate dynamic: %v", err)
	}
	raw, ok := rows[res.AggID]
	if !ok {
		t.Fatalf("missing id %q; got %v", res.AggID, rows)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["sku"] != "A1" {
		t.Fatalf("want sku=A1, got %q", got["sku"])
	}
}

func TestHydrate_SkipsMissing(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, err := NewToolkit(ff, reg, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	rows, err := tk.hydrate(ctx, "doc", []string{"nope"})
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %d", len(rows))
	}
}
