package fabriq

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

type fakeDDL struct {
	ensured     []string
	dropped     []string
	renamed     []string
	droppedCols []string
	failEnsure  bool
	guarded     map[string]bool
}

func newFakeDDL() *fakeDDL { return &fakeDDL{guarded: map[string]bool{}} }

func (f *fakeDDL) EnsureDynamic(_ context.Context, ent *registry.Entity) error {
	if f.failEnsure {
		return errors.New("boom")
	}
	f.ensured = append(f.ensured, ent.Spec.Name)
	return nil
}
func (f *fakeDDL) DropDynamicColumn(_ context.Context, table, column string) error {
	f.droppedCols = append(f.droppedCols, table+"."+column)
	return nil
}
func (f *fakeDDL) RenameDynamicColumn(_ context.Context, table, oldName, newName string) error {
	f.renamed = append(f.renamed, table+"."+oldName+"->"+newName)
	return nil
}
func (f *fakeDDL) DropDynamic(_ context.Context, table string) error {
	f.dropped = append(f.dropped, table)
	return nil
}
func (f *fakeDDL) AddGuardedTable(t string)    { f.guarded[t] = true }
func (f *fakeDDL) RemoveGuardedTable(t string) { delete(f.guarded, t) }

func spec(name string) registry.EntitySpec {
	return registry.EntitySpec{
		Name: name, Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{Table: "ds_" + name,
			Columns: []registry.DynamicColumn{{Name: "colour", Type: registry.ColText}}},
	}
}

func TestDefineDynamicRegistersEnsuresGuards(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	if err := defineDynamic(context.Background(), reg, ddl, spec("widget")); err != nil {
		t.Fatalf("define: %v", err)
	}
	if _, ok := reg.Get("widget"); !ok {
		t.Fatal("not registered")
	}
	if len(ddl.ensured) != 1 || !ddl.guarded["ds_widget"] {
		t.Fatalf("expected ensure + guard, got ensured=%v guarded=%v", ddl.ensured, ddl.guarded)
	}
}

func TestDefineDynamicRollsBackOnDDLFailure(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	ddl.failEnsure = true
	if err := defineDynamic(context.Background(), reg, ddl, spec("widget")); err == nil {
		t.Fatal("expected error from failing EnsureDynamic")
	}
	if _, ok := reg.Get("widget"); ok {
		t.Fatal("registry not rolled back after DDL failure")
	}
}

func TestDefineDynamicRejectsModelled(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	bad := registry.EntitySpec{Name: "x", Kind: registry.KindAggregate} // no Schema => not dynamic
	if err := defineDynamic(context.Background(), reg, ddl, bad); err == nil {
		t.Fatal("expected rejection of non-dynamic spec")
	}
}

func TestDropDynamicUnregistersAndDrops(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	_ = defineDynamic(context.Background(), reg, ddl, spec("widget"))
	if err := dropDynamic(context.Background(), reg, ddl, "widget"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, ok := reg.Get("widget"); ok {
		t.Fatal("still registered after drop")
	}
	if len(ddl.dropped) != 1 || ddl.guarded["ds_widget"] {
		t.Fatalf("expected table drop + unguard, dropped=%v guarded=%v", ddl.dropped, ddl.guarded)
	}
}

func TestAlterDynamicAddsColumnAndEnsures(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	if err := defineDynamic(context.Background(), reg, ddl, spec("widget")); err != nil {
		t.Fatalf("define: %v", err)
	}
	altered := spec("widget")
	altered.Schema.Columns = append(altered.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColText})
	if err := alterDynamic(context.Background(), reg, ddl, altered); err != nil {
		t.Fatalf("alter: %v", err)
	}
	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("not registered")
	}
	if !ent.Binding.HasColumn("size") {
		t.Fatalf("expected registry entity to have new column %q", "size")
	}
	if len(ddl.ensured) != 2 {
		t.Fatalf("expected 2 EnsureDynamic calls, got %v", ddl.ensured)
	}
}

func TestRenameDynamicFieldRenamesColumnAndReRegisters(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	if err := defineDynamic(context.Background(), reg, ddl, spec("widget")); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := renameDynamicField(context.Background(), reg, ddl, "widget", "colour", "color"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if len(ddl.renamed) != 1 {
		t.Fatalf("expected 1 RenameDynamicColumn call, got %v", ddl.renamed)
	}
	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("not registered")
	}
	if ent.Binding.HasColumn("colour") {
		t.Fatal("expected old column name to be gone")
	}
	if !ent.Binding.HasColumn("color") {
		t.Fatal("expected new column name to be present")
	}
}

func TestDropDynamicFieldDropsColumnAndReRegisters(t *testing.T) {
	reg := registry.New()
	ddl := newFakeDDL()
	s := spec("widget")
	s.Schema.Columns = append(s.Schema.Columns, registry.DynamicColumn{Name: "size", Type: registry.ColText})
	if err := defineDynamic(context.Background(), reg, ddl, s); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := dropDynamicField(context.Background(), reg, ddl, "widget", "size"); err != nil {
		t.Fatalf("drop field: %v", err)
	}
	if len(ddl.droppedCols) != 1 {
		t.Fatalf("expected 1 DropDynamicColumn call, got %v", ddl.droppedCols)
	}
	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("not registered")
	}
	if ent.Binding.HasColumn("size") {
		t.Fatal("expected dropped column to be gone")
	}
}
