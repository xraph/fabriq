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
// Validate attach to the loaded type. When the type came from the store and
// the caller's spec only adds columns or indexes (see additiveDynamicChange),
// the define runs as AlterDynamic instead. Any other difference still fails
// as registered twice.
func defineDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, spec registry.EntitySpec) error {
	_, err := defineDynamicSynced(ctx, reg, ddl, store, loaded, spec)
	return err
}

// defineDynamicSynced is defineDynamic that also returns the sync it ran
// first, if any, so the facade can record it as the last sync.
//
// With a store and a name this process has not registered, it syncs before
// defining: another instance may already have persisted the name. The synced
// type then goes through the usual checks, so an equal spec succeeds without
// a save and a different one fails as registered twice, leaving the other
// instance's row alone. A persisted row the sync had to skip (unreadable, a
// newer format, or a table it could not ensure) blocks the define rather than
// being overwritten.
func defineDynamicSynced(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, spec registry.EntitySpec) (*DynamicSyncResult, error) {
	if err := assertDynamicAggregate(spec); err != nil {
		return nil, err
	}
	var (
		encoded []byte
		synced  *DynamicSyncResult
	)
	if store != nil {
		if _, ok := reg.Get(spec.Name); !ok {
			res, err := syncPersistedDynamic(ctx, reg, ddl, store, loaded)
			if err != nil {
				return nil, fmt.Errorf("fabriq: sync persisted dynamic entities before defining %q: %w", spec.Name, err)
			}
			synced = &res
			if reason, skipped := res.Skipped[spec.Name]; skipped {
				return synced, fmt.Errorf("fabriq: dynamic entity %q has a persisted descriptor this process cannot load (%s)", spec.Name, reason)
			}
		}
		if cur, ok := reg.Get(spec.Name); ok && cur.Spec.Schema != nil && cur.Spec.Kind == registry.KindAggregate {
			if sameDynamicEncoding(cur.Spec, spec) {
				return synced, reg.Replace(spec)
			}
			// A type this facade loaded or persisted (never one registered in
			// code) that the caller only grew: treat the define as an alter.
			if _, owned := loaded[spec.Name]; owned && additiveDynamicChange(cur.Spec, spec) {
				return synced, alterDynamic(ctx, reg, ddl, store, loaded, spec)
			}
		}
		b, err := encodeDynamicSpec(spec)
		if err != nil {
			return synced, err
		}
		encoded = b
	}
	if err := reg.Register(spec); err != nil {
		return synced, err
	}
	ent, _ := reg.Get(spec.Name)
	if err := ddl.EnsureDynamic(ctx, ent); err != nil {
		_ = reg.Unregister(spec.Name) // rollback
		return synced, err
	}
	if store != nil {
		if err := saveDynamicSpec(ctx, store, loaded, spec.Name, encoded); err != nil {
			_ = reg.Unregister(spec.Name) // rollback
			return synced, err
		}
	}
	ddl.AddGuardedTable(spec.Schema.Table)
	return synced, nil
}

// alterDynamic: additive add-field via idempotent EnsureDynamic, then
// re-register, then persist. A save failure returns the error but keeps the
// new spec registered, because its DDL has already run. A type registered in
// code (not in loaded) is altered without writing a row.
func alterDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64, spec registry.EntitySpec) error {
	if err := assertDynamicAggregate(spec); err != nil {
		return err
	}
	prev, ok := reg.Get(spec.Name)
	if !ok {
		return fmt.Errorf("fabriq: cannot alter unknown entity %q", spec.Name)
	}
	// Only a type this facade persisted or loaded owns its row. A dynamic
	// type registered in code must never write one (see dropDynamic).
	_, owned := loaded[spec.Name]
	persist := store != nil && owned
	var encoded []byte
	if persist {
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
		// Rollback restores the descriptor. EnsureDynamic runs its DDL in one
		// transaction, so a failed ensure leaves the table as it was.
		_ = reg.Replace(prev.Spec)
		return err
	}
	if persist {
		return saveDynamicSpec(ctx, store, loaded, spec.Name, encoded)
	}
	return nil
}

// dropDynamic removes a dynamic type. With a store, and when this facade
// persisted or loaded the type, the persisted row goes first, so a crash between the steps leaves an orphan table rather than a
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
	// Only a type this facade persisted or loaded owns its row. A dynamic
	// type registered in code must never delete another instance's row.
	_, owned := loaded[name]
	persisted := store != nil && owned
	if persisted {
		if err := store.DeleteDynamicSpec(ctx, name); err != nil {
			return fmt.Errorf("fabriq: delete persisted dynamic entity %q: %w", name, err)
		}
	}
	if err := ddl.DropDynamic(ctx, table); err != nil {
		if persisted {
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

// replaceAndPersist swaps next into the registry and, with a store, saves it
// when this facade owns the row (the name is in loaded). A type registered in
// code never writes one. A save failure keeps next registered: the DDL behind
// it already ran.
func replaceAndPersist(ctx context.Context, reg *registry.Registry, store DynamicSpecStore, loaded map[string]int64, next registry.EntitySpec) error {
	if err := reg.Replace(next); err != nil {
		return err
	}
	if _, owned := loaded[next.Name]; store == nil || !owned {
		return nil
	}
	b, err := encodeDynamicSpec(next)
	if err != nil {
		return persistError(next.Name, err)
	}
	return saveDynamicSpec(ctx, store, loaded, next.Name, b)
}

// additiveDynamicChange reports whether next only grows cur: every column of
// cur is in next with the same type, NotNull and Default, every index of cur
// is in next unchanged, the table is the same, and so is every other persisted
// field (graph, edges, search, scopes, live, cache, NoTypeCheck). next may add
// columns and indexes, nothing else. The check projects next onto cur's
// columns and indexes and compares the persisted encodings, so it covers
// whatever the encoding covers.
func additiveDynamicChange(cur, next registry.EntitySpec) bool {
	if cur.Schema == nil || next.Schema == nil || cur.Schema.Table != next.Schema.Table {
		return false
	}
	cols := make(map[string]registry.DynamicColumn, len(next.Schema.Columns))
	for _, c := range next.Schema.Columns {
		cols[c.Name] = c
	}
	idx := make(map[string]registry.DynamicIndex, len(next.Schema.Indexes))
	for _, i := range next.Schema.Indexes {
		idx[i.Name] = i
	}
	sc := *next.Schema
	sc.Columns = make([]registry.DynamicColumn, 0, len(cur.Schema.Columns))
	for _, c := range cur.Schema.Columns {
		nc, ok := cols[c.Name]
		if !ok {
			return false
		}
		sc.Columns = append(sc.Columns, nc)
	}
	sc.Indexes = make([]registry.DynamicIndex, 0, len(cur.Schema.Indexes))
	for _, i := range cur.Schema.Indexes {
		ni, ok := idx[i.Name]
		if !ok {
			return false
		}
		sc.Indexes = append(sc.Indexes, ni)
	}
	projected := next
	projected.Schema = &sc
	return sameDynamicEncoding(cur, projected)
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
// type then succeeds without DDL and attaches those fields. When the
// registered type came from the store and the new spec only adds columns or
// indexes, DefineDynamic applies it as AlterDynamic would; any other change
// fails with "registered twice".
func (f *Fabriq) DefineDynamic(ctx context.Context, spec registry.EntitySpec) error {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return err
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	synced, err := defineDynamicSynced(ctx, f.reg, ddl, f.dynamicStore(), f.dynLoadedMap(), spec)
	if synced != nil {
		f.dynLast = *synced
	}
	return err
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
