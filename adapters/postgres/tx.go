package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// InTenantTx implements command.Store: one Postgres transaction, tenant
// stamped via SET LOCAL, row writes and outbox appends inside it.
func (a *Adapter) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	return a.inTenantTx(ctx, func(ptx *pgdriver.PgTx) error {
		return fn(ctx, &storeTx{ptx: ptx})
	})
}

type storeTx struct {
	ptx *pgdriver.PgTx
}

type versionRow struct {
	Version int64 `grove:"version"`
}

// CurrentVersion reads the stored aggregate version under FOR UPDATE so
// concurrent commands on the same aggregate serialize at the row.
func (t *storeTx) CurrentVersion(ctx context.Context, ent *registry.Entity, aggID string) (int64, error) {
	var rows []versionRow
	sql := fmt.Sprintf(`SELECT version FROM %s WHERE id = $1 FOR UPDATE`, quoteIdent(ent.Binding.Table))
	if err := t.ptx.NewRaw(sql, aggID).Scan(ctx, &rows); err != nil {
		return 0, fmt.Errorf("fabriq: current version of %s/%s: %w", ent.Spec.Name, aggID, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Version, nil
}

// ApplyChange writes the aggregate row from the stamped column values.
func (t *storeTx) ApplyChange(ctx context.Context, ent *registry.Entity, op command.Op, aggID string, version int64, vals map[string]any) error {
	switch op {
	case command.OpDelete:
		sql := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, quoteIdent(ent.Binding.Table))
		if _, err := t.ptx.NewRaw(sql, aggID).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: delete %s/%s: %w", ent.Spec.Name, aggID, err)
		}
		return nil
	case command.OpCreate:
		model := ent.Binding.NewModel()
		if err := ent.Binding.Populate(model, vals); err != nil {
			return err
		}
		if _, err := t.ptx.NewInsert(model).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: insert %s/%s: %w", ent.Spec.Name, aggID, err)
		}
		return nil
	case command.OpUpdate:
		model := ent.Binding.NewModel()
		if err := ent.Binding.Populate(model, vals); err != nil {
			return err
		}
		if _, err := t.ptx.NewUpdate(model).Where("id = ?", aggID).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: update %s/%s: %w", ent.Spec.Name, aggID, err)
		}
		return nil
	default:
		return fmt.Errorf("fabriq: unknown op %d", op)
	}
}

// AppendOutbox appends the envelope and notifies the relay. pg_notify
// fires on commit, so the wake-up cannot outrun the data.
func (t *storeTx) AppendOutbox(ctx context.Context, env event.Envelope) error {
	const insert = `INSERT INTO fabriq_outbox
		(id, tenant_id, aggregate, agg_id, version, type, at, payload_schema_version, payload, traceparent)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	if _, err := t.ptx.NewRaw(insert,
		env.ID, env.TenantID, env.Aggregate, env.AggID, env.Version, env.Type,
		env.At, env.PayloadSchemaVersion, []byte(env.Payload), env.Traceparent,
	).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: append outbox %s: %w", env.ID, err)
	}
	if _, err := t.ptx.NewRaw(`SELECT pg_notify('fabriq_outbox', $1)`, env.ID).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: notify outbox: %w", err)
	}
	return nil
}
