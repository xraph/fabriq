package registry_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestBindingDynColumn(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "orders", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
			},
		},
	})
	ent, ok := r.Get("orders")
	if !ok {
		t.Fatal("orders not registered")
	}
	dc, ok := ent.Binding.DynColumn("qty")
	if !ok || dc.Type != registry.ColInt {
		t.Fatalf("qty: ok=%v type=%v", ok, dc.Type)
	}
	if _, ok := ent.Binding.DynColumn("nope"); ok {
		t.Fatal("unknown column should return ok=false")
	}
}
