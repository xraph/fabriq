package fabriq

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

type fakeDDL struct {
	ensured    []string
	dropped    []string
	failEnsure bool
	guarded    map[string]bool
}

func newFakeDDL() *fakeDDL { return &fakeDDL{guarded: map[string]bool{}} }

func (f *fakeDDL) EnsureDynamic(_ context.Context, ent *registry.Entity) error {
	if f.failEnsure {
		return errors.New("boom")
	}
	f.ensured = append(f.ensured, ent.Spec.Name)
	return nil
}
func (f *fakeDDL) DropDynamicColumn(_ context.Context, _, _ string) error      { return nil }
func (f *fakeDDL) RenameDynamicColumn(_ context.Context, _, _, _ string) error { return nil }
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
