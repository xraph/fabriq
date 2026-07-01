package registry

import "testing"

func dynSpec(name string, cols ...DynamicColumn) EntitySpec {
	return EntitySpec{
		Name: name,
		Kind: KindAggregate,
		Schema: &DynamicSchema{
			Table:   "ds_" + name,
			Columns: cols,
		},
	}
}

func TestReplaceRebindsDynamicEntity(t *testing.T) {
	r := New()
	if err := r.Register(dynSpec("widget", DynamicColumn{Name: "colour", Type: ColText})); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Replace with an added column.
	err := r.Replace(dynSpec("widget",
		DynamicColumn{Name: "colour", Type: ColText},
		DynamicColumn{Name: "size", Type: ColInt},
	))
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	ent, ok := r.Get("widget")
	if !ok {
		t.Fatal("widget gone after replace")
	}
	if !ent.Binding.HasColumn("size") {
		t.Fatal("replace did not rebind: missing new column 'size'")
	}
}

func TestReplaceRejectsUnknown(t *testing.T) {
	r := New()
	if err := r.Replace(dynSpec("ghost")); err == nil {
		t.Fatal("expected error replacing unknown entity")
	}
}

func TestUnregisterRemovesDynamic(t *testing.T) {
	r := New()
	_ = r.Register(dynSpec("widget", DynamicColumn{Name: "colour", Type: ColText}))
	if err := r.Unregister("widget"); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if _, ok := r.Get("widget"); ok {
		t.Fatal("widget still present after unregister")
	}
}

func TestUnregisterRejectsUnknown(t *testing.T) {
	r := New()
	if err := r.Unregister("ghost"); err == nil {
		t.Fatal("expected error unregistering unknown entity")
	}
}
