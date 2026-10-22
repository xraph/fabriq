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

// defineDynamic: register -> ensure table -> persist -> guard. Rolls the
// registry back if the DDL or the save fails, so we never leave a
// registered-but-tableless (or, with a store, unpersisted) type. A failed save
// leaves the table in place; a later define of the same spec reuses it.
//
// With a store, defining a spec whose persisted encoding equals the one
// already registered is a no-op success, so a library app can repeat its
// boot-time defines over types the Open-time sync already loaded. That path
// swaps in the caller's spec (no DDL, no save) so in-process fields such as
// Validate attach to the loaded type.
func defineDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, spec registry.EntitySpec) error {
	if err := assertDynamicAggregate(spec); err != nil {
		return err
	}
	var encoded []byte
	if store != nil {
		if cur, ok := reg.Get(spec.Name); ok && cur.Spec.Schema != nil && cur.Spec.Kind == registry.KindAggregate && sameDynamicEncoding(cur.Spec, spec) {
			return reg.Replace(spec)
		}
		b, err := encodeDynamicSpec(spec)
		if err != nil {
			return err
		}
		encoded = b
	}
	if err := reg.Register(spec); err != nil {
		return err
	}
	ent, _ := reg.Get(spec.Name)
	if err := ddl.EnsureDynamic(ctx, ent); err != nil {
		_ = reg.Unregister(spec.Name) // rollback
		return err
	}
	if store != nil {
		if err := saveDynamicSpec(ctx, store, loaded, spec.Name, encoded); err != nil {
			_ = reg.Unregister(spec.Name) // rollback
			return err
		}
	}
	ddl.AddGuardedTable(spec.Schema.Table)
	return nil
}

// alterDynamic: additive add-field via idempotent EnsureDynamic, then
// re-register, then persist. A save failure returns the error but keeps the
// new spec registered, because its DDL has already run.
func alterDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, spec registry.EntitySpec) error {
	if err := assertDynamicAggregate(spec); err != nil {
		return err
	}
	prev, ok := reg.Get(spec.Name)
	if !ok {
		return fmt.Errorf("fabriq: cannot alter unknown entity %q", spec.Name)
	}
	var encoded []byte
	if store != nil {
		b, err := encodeDynamicSpec(spec)
		if err != nil {
			return err
		}
		encoded = b
	}
	if err := reg.Replace(spec); err != nil {
		return err
	}
	ent, _ := reg.Get(spec.Name)
	if err := ddl.EnsureDynamic(ctx, ent); err != nil {
		// Rollback restores the descriptor only. EnsureDynamic applies additive
		// DDL statement-by-statement (non-transactional, Task 3), so a partial
		// failure may leave already-added columns physically present; this is
		// bounded (additive-only, no data loss) and matches EnsureDynamic's
		// documented additive-evolution contract.
		_ = reg.Replace(prev.Spec)
		return err
	}
	if store != nil {
		return saveDynamicSpec(ctx, store, loaded, spec.Name, encoded)
	}
	return nil
}

// dropDynamic removes a dynamic type. With a store the persisted row goes
// first, so a crash between the steps leaves an orphan table rather than a
// row that would bring the type back without its data. If the table drop
// fails, the previous spec is saved again (best effort) and the DDL error
// returned.
func dropDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, name string) error {
	ent, ok := reg.Get(name)
	if !ok {
		return fmt.Errorf("fabriq: cannot drop unknown entity %q", name)
	}
	if err := assertDynamicAggregate(ent.Spec); err != nil {
		return err
	}
	table := ent.Spec.Schema.Table
	if store != nil {
		if err := store.DeleteDynamicSpec(ctx, name); err != nil {
			return fmt.Errorf("fabriq: delete persisted dynamic entity %q: %w", name, err)
		}
	}
	if err := ddl.DropDynamic(ctx, table); err != nil {
		if store != nil {
			if b, eerr := encodeDynamicSpec(ent.Spec); eerr == nil {
				_ = saveDynamicSpec(ctx, store, loaded, name, b)
			}
		}
		return err
	}
	ddl.RemoveGuardedTable(table)
	delete(loaded, name)
	return reg.Unregister(name)
}

func renameDynamicField(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, name, oldCol, newCol string) error {
	ent, ok := reg.Get(name)
	if !ok {
		return fmt.Errorf("fabriq: unknown dynamic entity %q", name)
	}
	if err := assertDynamicAggregate(ent.Spec); err != nil {
		return err
	}
	if err := ddl.RenameDynamicColumn(ctx, ent.Spec.Schema.Table, oldCol, newCol); err != nil {
		return err
	}
	next := cloneSpecRenamingColumn(ent.Spec, oldCol, newCol)
	return replaceAndPersist(ctx, reg, store, loaded, next)
}

func dropDynamicField(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, name, col string) error {
	ent, ok := reg.Get(name)
	if !ok {
		return fmt.Errorf("fabriq: unknown dynamic entity %q", name)
	}
	if err := assertDynamicAggregate(ent.Spec); err != nil {
		return err
	}
	if err := ddl.DropDynamicColumn(ctx, ent.Spec.Schema.Table, col); err != nil {
		return err
	}
	next := cloneSpecDroppingColumn(ent.Spec, col)
	return replaceAndPersist(ctx, reg, store, loaded, next)
}

// replaceAndPersist swaps next into the registry and, with a store, saves it.
// A save failure keeps next registered: the DDL behind it already ran.
func replaceAndPersist(ctx context.Context, reg *registry.Registry, store DynamicSpecStore, loaded map[string]int64, next registry.EntitySpec) error {
	if err := reg.Replace(next); err != nil {
		return err
	}
	if store == nil {
		return nil
	}
	b, err := encodeDynamicSpec(next)
	if err != nil {
		return persistError(next.Name, err)
	}
	return saveDynamicSpec(ctx, store, loaded, next.Name, b)
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
//
// When Config.Dynamic.Persist is on, the descriptor is also saved to
// fabriq_dynamic_entity and reloaded the next time fabriq opens. Only the
// declarative part persists: Validate, Embed, Distill, Analytics, Insights,
// Metrics and CRDT stay in-process, so an app that needs them defines the type
// again at boot. Defining a spec whose persisted part equals the registered
// type then succeeds without DDL and attaches those fields.
func (f *Fabriq) DefineDynamic(ctx context.Context, spec registry.EntitySpec) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return defineDynamic(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), spec)
}

// AlterDynamic additively evolves a dynamic type (new columns/indexes only).
func (f *Fabriq) AlterDynamic(ctx context.Context, spec registry.EntitySpec) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return alterDynamic(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), spec)
}

// RenameDynamicField renames a domain column of a dynamic type.
func (f *Fabriq) RenameDynamicField(ctx context.Context, typeName, oldCol, newCol string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return renameDynamicField(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), typeName, oldCol, newCol)
}

// DropDynamicField drops a domain column of a dynamic type.
func (f *Fabriq) DropDynamicField(ctx context.Context, typeName, col string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return dropDynamicField(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), typeName, col)
}

// DropDynamic drops a dynamic type (its table, registry entry and, when
// persistence is on, its persisted descriptor).
func (f *Fabriq) DropDynamic(ctx context.Context, typeName string) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return dropDynamic(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), typeName)
}
