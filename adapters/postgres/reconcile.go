package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

type idVersionRow struct {
	ID      string `grove:"id"`
	Version int64  `grove:"version"`
}

// AggregateVersions reads id -> version for one entity of a tenant — the
// reconciler's truth side (projection.TruthVersions).
func (a *Adapter) AggregateVersions(ctx context.Context, tenantID, entity string) (map[string]int64, error) {
	ent, err := a.entity(entity)
	if err != nil {
		return nil, err
	}
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	err = a.inTenantTx(tctx, func(tx *pgdriver.PgTx) error {
		var rows []idVersionRow
		sql := fmt.Sprintf(`SELECT id, version FROM %s WHERE tenant_id = $1`, quoteIdent(ent.Binding.Table))
		if scanErr := tx.NewRaw(sql, tenantID).Scan(tctx, &rows); scanErr != nil {
			return scanErr
		}
		for _, r := range rows {
			out[r.ID] = r.Version
		}
		return nil
	})
	return out, err
}

// Repair heals one drift through the ordinary pipeline
// (projection.RepairFunc):
//
//   - missing/stale: upsert the aggregate's CURRENT state as its
//     version's event and mark it unpublished — the relay republishes,
//     version-gated consumers converge.
//   - zombie (row gone): emit a synthetic <entity>.deleted one version
//     past what the projection holds, so the delete applies everywhere.
//
// Reconciliation never writes a projection engine directly.
func (a *Adapter) Repair(ctx context.Context, tenantID string, d projection.Drift) error {
	ent, err := a.entity(d.Entity)
	if err != nil {
		return err
	}
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return err
	}

	var env event.Envelope
	if d.TruthVersion > 0 {
		vals, verr := a.rowValues(tctx, ent, d.AggID)
		if verr != nil {
			return verr
		}
		payload, merr := json.Marshal(vals)
		if merr != nil {
			return merr
		}
		env = event.Envelope{
			ID: event.NewID(), TenantID: tenantID, Aggregate: d.Entity, AggID: d.AggID,
			Version: d.TruthVersion, Type: registry.EventType(d.Entity, registry.VerbUpdated),
			At: time.Now().UTC(), PayloadSchemaVersion: 1, Payload: payload,
		}
	} else {
		env = event.Envelope{
			ID: event.NewID(), TenantID: tenantID, Aggregate: d.Entity, AggID: d.AggID,
			Version: d.ProjectedVersion + 1, Type: registry.EventType(d.Entity, registry.VerbDeleted),
			At: time.Now().UTC(), PayloadSchemaVersion: 1, Payload: json.RawMessage(`{}`),
		}
	}

	const upsert = `INSERT INTO fabriq_outbox
		(id, tenant_id, aggregate, agg_id, version, type, at, payload_schema_version, payload, traceparent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '')
		ON CONFLICT (tenant_id, aggregate, agg_id, version)
		DO UPDATE SET published_at = NULL, stream_id = '', payload = EXCLUDED.payload, at = EXCLUDED.at`
	if _, err := a.pg.Exec(ctx, upsert,
		env.ID, env.TenantID, env.Aggregate, env.AggID, env.Version, env.Type,
		env.At, env.PayloadSchemaVersion, []byte(env.Payload)); err != nil {
		return fmt.Errorf("fabriq: repair republish %s/%s: %w", d.Entity, d.AggID, err)
	}
	if _, err := a.pg.Exec(ctx, `SELECT pg_notify('fabriq_outbox', $1)`, env.ID); err != nil {
		return err
	}
	return nil
}

// rowValues loads one aggregate row as column-keyed values.
func (a *Adapter) rowValues(tctx context.Context, ent *registry.Entity, aggID string) (map[string]any, error) {
	model := ent.Binding.NewModel()
	var vals map[string]any
	err := a.inTenantTx(tctx, func(tx *pgdriver.PgTx) error {
		if err := tx.NewSelect(model).Where("id = ?", aggID).Limit(1).Scan(tctx); err != nil {
			return err
		}
		v, err := ent.Binding.ValuesByColumn(model)
		if err != nil {
			return err
		}
		vals = v
		return nil
	})
	return vals, err
}
