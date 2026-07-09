package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// SnapshotEntities streams every aggregate row of a tenant as synthetic
// <entity>.updated envelopes at the row's CURRENT version and shape —
// the rebuild source (projections are always rebuilt from Postgres, never
// from another projection). Rows are paged in id order inside stamped
// transactions; because appliers are pure and sinks version-gate, a row
// that changes mid-snapshot is healed by the live catch-up applies.
func (a *Adapter) SnapshotEntities(ctx context.Context, tenantID string, fn func(env event.Envelope) error) error {
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	for _, ent := range a.reg.All() {
		if ent.Spec.Kind != registry.KindAggregate {
			continue
		}
		if err := a.snapshotEntity(tctx, tenantID, ent, fn); err != nil {
			return fmt.Errorf("fabriq: snapshot %s: %w", ent.Spec.Name, err)
		}
	}
	return nil
}

func (a *Adapter) snapshotEntity(tctx context.Context, tenantID string, ent *registry.Entity, fn func(env event.Envelope) error) error {
	// Dynamic (DynamicSchema) entities have no Go model type, so the reflection
	// path below (reflect.PointerTo(ModelType())) would panic. Read them
	// map-natively instead, exactly like the RelationalQuerier dynamic reads.
	if ent.Binding.IsDynamic() {
		return a.snapshotDynamicEntity(tctx, tenantID, ent, fn)
	}
	const pageSize = 500
	lastID := ""
	sliceType := reflect.SliceOf(reflect.PointerTo(ent.Binding.ModelType()))

	for {
		page := reflect.New(sliceType)
		err := a.inTenantTx(tctx, func(tx *pgdriver.PgTx) error {
			return tx.NewSelect(page.Interface()).
				Where(registry.ColumnTenant+" = ?", tenantID).
				Where(registry.ColumnID+" > ?", lastID).
				OrderExpr(`"id" ASC`).
				Limit(pageSize).
				Scan(tctx)
		})
		if err != nil {
			return err
		}
		rows := page.Elem()
		for i := 0; i < rows.Len(); i++ {
			model := rows.Index(i).Interface()
			vals, err := ent.Binding.ValuesByColumn(model)
			if err != nil {
				return err
			}
			env, err := snapshotEnvelope(tenantID, ent, vals)
			if err != nil {
				return err
			}
			lastID = env.AggID
			if err := fn(env); err != nil {
				return err
			}
		}
		if rows.Len() < pageSize {
			return nil
		}
	}
}

// snapshotDynamicEntity streams a dynamic entity's rows the same way
// snapshotEntity does for Go-model entities, but reads each ds_ table row into a
// map[string]any (there is no struct to reflect). It pages by id inside stamped
// dynamic-tenant transactions — the same read shape RelationalQuerier.Get/List
// use — so live and rebuilt projections of a dynamic aggregate stay identical.
func (a *Adapter) snapshotDynamicEntity(tctx context.Context, tenantID string, ent *registry.Entity, fn func(env event.Envelope) error) error {
	if !ddlValid(ent.Binding.Table) {
		return fmt.Errorf("fabriq: dynamic table name %q failed ddl validation", ent.Binding.Table)
	}
	const pageSize = 500
	table := quoteIdent(ent.Binding.Table)
	lastID := ""
	for {
		var maps []map[string]any
		err := a.inDynamicTenantTx(tctx, func(tid string, tx driver.Tx) error {
			sql := fmt.Sprintf(
				`SELECT * FROM %s WHERE %s = $1 AND %s > $2 ORDER BY "id" ASC LIMIT %d`,
				table, registry.ColumnTenant, registry.ColumnID, pageSize)
			rows, qerr := tx.Query(tctx, sql, tid, lastID)
			if qerr != nil {
				return qerr
			}
			m, serr := scanMaps(rows)
			if serr != nil {
				return serr
			}
			maps = m
			return nil
		})
		if err != nil {
			return err
		}
		for _, vals := range maps {
			env, err := snapshotEnvelope(tenantID, ent, vals)
			if err != nil {
				return err
			}
			lastID = env.AggID
			if err := fn(env); err != nil {
				return err
			}
		}
		if len(maps) < pageSize {
			return nil
		}
	}
}

func snapshotEnvelope(tenantID string, ent *registry.Entity, vals map[string]any) (event.Envelope, error) {
	id, _ := vals[registry.ColumnID].(string)
	version, _ := vals[registry.ColumnVersion].(int64)
	if id == "" || version < 1 {
		return event.Envelope{}, fmt.Errorf("row of %s lacks id/version (%v)", ent.Spec.Name, vals)
	}
	payload, err := json.Marshal(vals)
	if err != nil {
		return event.Envelope{}, err
	}
	return event.Envelope{
		ID:        event.NewID(),
		TenantID:  tenantID,
		Aggregate: ent.Spec.Name,
		AggID:     id,
		Version:   version,
		Type:      registry.EventType(ent.Spec.Name, registry.VerbUpdated),
		At:        time.Now().UTC(),
		// Snapshot payloads are the CURRENT table shape: the rebuilder
		// applies them without the upcaster chain.
		PayloadSchemaVersion: 1,
		Payload:              payload,
	}, nil
}
