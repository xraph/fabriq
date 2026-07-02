package registry_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func dynSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "orders", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
				{Name: "meta", Type: registry.ColJSON},
			},
		},
	}
}

func TestDynamic_BindsWithStructuralColumns(t *testing.T) {
	r := registry.New()
	if err := r.Register(dynSpec()); err != nil {
		t.Fatalf("register dynamic: %v", err)
	}
	ent, ok := r.Get("orders")
	if !ok {
		t.Fatal("entity not registered")
	}
	b := ent.Binding
	for _, c := range []string{"id", "tenant_id", "version", "sku", "qty", "meta"} {
		if !b.HasColumn(c) {
			t.Fatalf("missing column %q", c)
		}
	}
	req := b.Required()
	if len(req) != 1 || req[0] != "sku" {
		t.Fatalf("Required = %v, want [sku]", req)
	}
}

func TestDynamic_ValuesByColumnAcceptsMap(t *testing.T) {
	r := registry.New()
	if err := r.Register(dynSpec()); err != nil {
		t.Fatal(err)
	}
	ent, _ := r.Get("orders")
	vals, err := ent.Binding.ValuesByColumn(map[string]any{"sku": "A1", "qty": 3, "unknown": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if vals["sku"] != "A1" || vals["qty"] != 3 {
		t.Fatalf("vals = %v", vals)
	}
	if _, leaked := vals["unknown"]; leaked {
		t.Fatalf("unknown column must be dropped: %v", vals)
	}
}

func TestDynamic_RejectsModelAndSchemaTogether(t *testing.T) {
	r := registry.New()
	s := dynSpec()
	s.Model = (*struct{ X int })(nil)
	if err := r.Register(s); err == nil {
		t.Fatal("Model + Schema must be mutually exclusive")
	}
}

func TestDynamic_RejectsRedeclaredStructuralColumn(t *testing.T) {
	for _, name := range []string{registry.ColumnID, registry.ColumnTenant, registry.ColumnVersion} {
		r := registry.New()
		s := dynSpec()
		s.Schema.Columns = append(s.Schema.Columns, registry.DynamicColumn{Name: name, Type: registry.ColText})
		if err := r.Register(s); err == nil {
			t.Fatalf("redeclaring reserved structural column %q must error", name)
		}
	}
}

// scope_id is structural-when-present but consumer-declared, so it must NOT be
// rejected as a reserved name — a dynamic entity may opt into secondary scoping
// by declaring it.
func TestDynamic_AllowsScopeColumn(t *testing.T) {
	r := registry.New()
	s := dynSpec()
	s.Schema.Columns = append(s.Schema.Columns, registry.DynamicColumn{Name: registry.ColumnScope, Type: registry.ColText})
	if err := r.Register(s); err != nil {
		t.Fatalf("declaring scope_id must be allowed, got %v", err)
	}
	ent, _ := r.Get("orders")
	if !ent.Binding.HasColumn(registry.ColumnScope) {
		t.Fatal("scope_id not bound")
	}
}

func TestIsReservedColumn(t *testing.T) {
	for _, name := range []string{registry.ColumnID, registry.ColumnTenant, registry.ColumnVersion} {
		if !registry.IsReservedColumn(name) {
			t.Errorf("IsReservedColumn(%q) = false, want true", name)
		}
	}
	for _, name := range []string{registry.ColumnScope, "sku", "tenant", "id2", ""} {
		if registry.IsReservedColumn(name) {
			t.Errorf("IsReservedColumn(%q) = true, want false", name)
		}
	}
}

func TestDynamic_RejectsBadIdentifier(t *testing.T) {
	r := registry.New()
	s := dynSpec()
	s.Schema.Columns = append(s.Schema.Columns, registry.DynamicColumn{Name: "bad-col", Type: registry.ColText})
	if err := r.Register(s); err == nil {
		t.Fatal("invalid column identifier must error")
	}
}

func TestDynamic_ValuesByColumnRejectsNilAndNonMap(t *testing.T) {
	r := registry.New()
	if err := r.Register(dynSpec()); err != nil {
		t.Fatal(err)
	}
	ent, _ := r.Get("orders")
	if _, err := ent.Binding.ValuesByColumn(nil); err == nil {
		t.Fatal("nil payload must error")
	}
	if _, err := ent.Binding.ValuesByColumn("not a map"); err == nil {
		t.Fatal("non-map payload must error")
	}
}

func TestDynamic_ZeroDomainColumnsIsValid(t *testing.T) {
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name: "lookup", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{Table: "ds_lookup"},
	}); err != nil {
		t.Fatalf("zero-column dynamic entity must be valid: %v", err)
	}
	ent, _ := r.Get("lookup")
	for _, c := range []string{"id", "tenant_id", "version"} {
		if !ent.Binding.HasColumn(c) {
			t.Fatalf("missing structural column %q", c)
		}
	}
}
