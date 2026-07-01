package fabriq

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/fabriq/core/registry"
)

// ErrDynamicUnavailable is returned by the dynamic-schema facade methods when
// the Fabriq was assembled without a Postgres store (e.g. via New for tests).
var ErrDynamicUnavailable = errors.New("fabriq: dynamic schema management requires Open with a Postgres store")

// DynamicDDL is the physical-schema seam the dynamic lifecycle facade depends
// on; *adapters/postgres.Adapter satisfies it.
type DynamicDDL interface {
	EnsureDynamic(context.Context, *registry.Entity) error
	DropDynamicColumn(ctx context.Context, table, column string) error
	RenameDynamicColumn(ctx context.Context, table, oldName, newName string) error
	DropDynamic(ctx context.Context, table string) error
	AddGuardedTable(table string)
	RemoveGuardedTable(table string)
}

func assertDynamicAggregate(spec registry.EntitySpec) error {
	if spec.Schema == nil {
		return fmt.Errorf("fabriq: entity %q is not a dynamic entity (Schema is nil)", spec.Name)
	}
	if spec.Kind != registry.KindAggregate {
		return fmt.Errorf("fabriq: dynamic entity %q must be KindAggregate", spec.Name)
	}
	return nil
}

// defineDynamic: register -> ensure table -> guard. Rolls the registry back if
// the DDL step fails, so we never leave a registered-but-tableless type.
func defineDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, spec registry.EntitySpec) error {
	if err := assertDynamicAggregate(spec); err != nil {
		return err
	}
	if err := reg.Register(spec); err != nil {
		return err
	}
	ent, _ := reg.Get(spec.Name)
	if err := ddl.EnsureDynamic(ctx, ent); err != nil {
		_ = reg.Unregister(spec.Name) // rollback
		return err
	}
	ddl.AddGuardedTable(spec.Schema.Table)
	return nil
}

// alterDynamic: additive add-field via idempotent EnsureDynamic, then re-register.
func alterDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, spec registry.EntitySpec) error {
	if err := assertDynamicAggregate(spec); err != nil {
		return err
	}
	prev, ok := reg.Get(spec.Name)
	if !ok {
		return fmt.Errorf("fabriq: cannot alter unknown entity %q", spec.Name)
	}
	if err := reg.Replace(spec); err != nil {
		return err
	}
	ent, _ := reg.Get(spec.Name)
	if err := ddl.EnsureDynamic(ctx, ent); err != nil {
		_ = reg.Replace(prev.Spec) // rollback to previous spec
		return err
	}
	return nil
}

func dropDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, name string) error {
	ent, ok := reg.Get(name)
	if !ok {
		return fmt.Errorf("fabriq: cannot drop unknown entity %q", name)
	}
	if ent.Spec.Schema == nil {
		return fmt.Errorf("fabriq: entity %q is not dynamic", name)
	}
	table := ent.Spec.Schema.Table
	if err := ddl.DropDynamic(ctx, table); err != nil {
		return err
	}
	ddl.RemoveGuardedTable(table)
	return reg.Unregister(name)
}

func renameDynamicField(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, name, oldCol, newCol string) error {
	ent, ok := reg.Get(name)
	if !ok || ent.Spec.Schema == nil {
		return fmt.Errorf("fabriq: unknown dynamic entity %q", name)
	}
	if err := ddl.RenameDynamicColumn(ctx, ent.Spec.Schema.Table, oldCol, newCol); err != nil {
		return err
	}
	next := cloneSpecRenamingColumn(ent.Spec, oldCol, newCol)
	return reg.Replace(next)
}

func dropDynamicField(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, name, col string) error {
	ent, ok := reg.Get(name)
	if !ok || ent.Spec.Schema == nil {
		return fmt.Errorf("fabriq: unknown dynamic entity %q", name)
	}
	if err := ddl.DropDynamicColumn(ctx, ent.Spec.Schema.Table, col); err != nil {
		return err
	}
	next := cloneSpecDroppingColumn(ent.Spec, col)
	return reg.Replace(next)
}

// cloneSpecRenamingColumn returns a copy of spec with the given schema column
// renamed (the Schema is deep-copied so the registry's stored spec is untouched
// until Replace succeeds).
func cloneSpecRenamingColumn(spec registry.EntitySpec, oldCol, newCol string) registry.EntitySpec {
	cp := spec
	cols := make([]registry.DynamicColumn, len(spec.Schema.Columns))
	copy(cols, spec.Schema.Columns)
	for i := range cols {
		if cols[i].Name == oldCol {
			cols[i].Name = newCol
		}
	}
	sc := *spec.Schema
	sc.Columns = cols
	cp.Schema = &sc
	return cp
}

func cloneSpecDroppingColumn(spec registry.EntitySpec, col string) registry.EntitySpec {
	cp := spec
	cols := make([]registry.DynamicColumn, 0, len(spec.Schema.Columns))
	for _, c := range spec.Schema.Columns {
		if c.Name != col {
			cols = append(cols, c)
		}
	}
	sc := *spec.Schema
	sc.Columns = cols
	cp.Schema = &sc
	return cp
}

// --- facade wrappers on *Fabriq ---

func (f *Fabriq) dynamicDDL() (DynamicDDL, error) {
	if f.stores == nil || f.stores.Postgres == nil {
		return nil, ErrDynamicUnavailable
	}
	return f.stores.Postgres, nil
}

// DefineDynamic registers a new dynamic entity type and creates its table.
func (f *Fabriq) DefineDynamic(ctx context.Context, spec registry.EntitySpec) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	return defineDynamic(ctx, f.reg, ddl, spec)
}

// AlterDynamic additively evolves a dynamic type (new columns/indexes only).
func (f *Fabriq) AlterDynamic(ctx context.Context, spec registry.EntitySpec) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	return alterDynamic(ctx, f.reg, ddl, spec)
}

// RenameDynamicField renames a domain column of a dynamic type.
func (f *Fabriq) RenameDynamicField(ctx context.Context, typeName, oldCol, newCol string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	return renameDynamicField(ctx, f.reg, ddl, typeName, oldCol, newCol)
}

// DropDynamicField drops a domain column of a dynamic type.
func (f *Fabriq) DropDynamicField(ctx context.Context, typeName, col string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	return dropDynamicField(ctx, f.reg, ddl, typeName, col)
}

// DropDynamic drops a dynamic type (its table and registry entry).
func (f *Fabriq) DropDynamic(ctx context.Context, typeName string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	return dropDynamic(ctx, f.reg, ddl, typeName)
}
