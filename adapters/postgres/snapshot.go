package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

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
