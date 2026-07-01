package registry_test

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func coerceRowDynEntity(t *testing.T) *registry.Entity {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "cr_widget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_cr_widget",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText},
				{Name: "qty", Type: registry.ColInt},
			},
		},
	})
	ent, ok := r.Get("cr_widget")
	if !ok {
		t.Fatal("cr_widget not registered")
	}
	return ent
}

func TestCoerceRow_CoercesInPlace(t *testing.T) {
	ent := coerceRowDynEntity(t)
	vals := map[string]any{"sku": "A1", "qty": float64(3)} // JSON number
	if err := registry.CoerceRow(ent, vals); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, ok := vals["qty"].(int64); !ok || got != 3 {
		t.Fatalf("qty must coerce to int64(3), got %#v", vals["qty"])
	}
	if vals["sku"] != "A1" {
		t.Fatalf("sku must stay %q, got %#v", "A1", vals["sku"])
	}
}

func TestCoerceRow_RejectsMismatchNamingColumn(t *testing.T) {
	ent := coerceRowDynEntity(t)
	err := registry.CoerceRow(ent, map[string]any{"qty": "not-a-number"})
	if err == nil || !strings.Contains(err.Error(), "qty") || !strings.Contains(err.Error(), "cr_widget") {
		t.Fatalf("want error naming entity and column, got %v", err)
	}
}

func TestCoerceRow_SkipsStructuralColumns(t *testing.T) {
	ent := coerceRowDynEntity(t)
	// A mistyped structural column must be ignored (stamped later), not rejected.
	vals := map[string]any{"qty": int64(1), "version": "not-an-int"}
	if err := registry.CoerceRow(ent, vals); err != nil {
		t.Fatalf("structural column must be skipped, got %v", err)
	}
}

func TestCoerceRow_NoopForGoModel(t *testing.T) {
	// embDoc is a grove-tagged model defined in embed_spec_test.go (same package).
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "cr_model", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
	})
	ent, ok := r.Get("cr_model")
	if !ok {
		t.Fatal("cr_model not registered")
	}
	// Even a "mistyped" value is left untouched — Go-model entities are typed by
	// their struct, so CoerceRow must not touch them.
	vals := map[string]any{"title": 123}
	if err := registry.CoerceRow(ent, vals); err != nil {
		t.Fatalf("CoerceRow must be a no-op for Go-model entities, got %v", err)
	}
	if vals["title"] != 123 {
		t.Fatalf("value must be untouched, got %#v", vals["title"])
	}
}
