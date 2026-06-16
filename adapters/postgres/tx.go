package postgres

import (
	"context"
	"fmt"
	"strings"

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
func (t *storeTx) ApplyChange(ctx context.Context, ent *registry.Entity, op command.Op, aggID string, _ int64, vals map[string]any) error {
	switch op {
	case command.OpDelete:
		sql := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, quoteIdent(ent.Binding.Table))
		if _, err := t.ptx.NewRaw(sql, aggID).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: delete %s/%s: %w", ent.Spec.Name, aggID, err)
		}
		return nil
	case command.OpCreate:
		if ent.Binding.IsDynamic() {
			return t.dynInsert(ctx, ent, vals)
		}
		model := ent.Binding.NewModel()
		if err := ent.Binding.Populate(model, vals); err != nil {
			return err
		}
		if _, err := t.ptx.NewInsert(model).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: insert %s/%s: %w", ent.Spec.Name, aggID, err)
		}
		return nil
	case command.OpUpdate:
		if ent.Binding.IsDynamic() {
			return t.dynUpdate(ctx, ent, aggID, vals)
		}
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
// Exec runs a raw statement inside the command transaction — the escape hatch
// a LifecycleHook uses to write atomically with the aggregate change. The tx is
// already tenant-stamped (SET LOCAL app.tenant_id), so RLS-guarded side tables
// are scoped to the calling tenant.
func (t *storeTx) Exec(ctx context.Context, sql string, args ...any) error {
	if _, err := t.ptx.NewRaw(sql, args...).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: tx exec: %w", err)
	}
	return nil
}

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

// dynInsert builds a parameterized INSERT from the column-keyed vals map.
// Column order follows ent.Binding.Columns so the SQL is deterministic.
// Every interpolated identifier is validated with ddlValid at the SQL boundary.
func (t *storeTx) dynInsert(ctx context.Context, ent *registry.Entity, vals map[string]any) error {
	if !ddlValid(ent.Binding.Table) {
		return fmt.Errorf("fabriq: invalid dynamic table %q", ent.Binding.Table)
	}
	cols := make([]string, 0, len(vals))
	args := make([]any, 0, len(vals))
	ph := make([]string, 0, len(vals))
	for _, c := range ent.Binding.Columns {
		v, ok := vals[c]
		if !ok {
			continue
		}
		if !ddlValid(c) {
			return fmt.Errorf("fabriq: invalid column %q for %s", c, ent.Binding.Table)
		}
		cols = append(cols, quoteIdent(c))
		args = append(args, v)
		ph = append(ph, fmt.Sprintf("$%d", len(args)))
	}
	if len(cols) == 0 {
		return fmt.Errorf("fabriq: dynInsert %s: no columns to insert", ent.Binding.Table)
	}
	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		quoteIdent(ent.Binding.Table), strings.Join(cols, ", "), strings.Join(ph, ", "))
	if _, err := t.ptx.NewRaw(sql, args...).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: insert %s: %w", ent.Binding.Table, err)
	}
	return nil
}

// dynUpdate builds a parameterized UPDATE from the column-keyed vals map.
// The id column is excluded from the SET clause and used only in the WHERE.
// Column order follows ent.Binding.Columns so the SQL is deterministic.
// Every interpolated identifier is validated with ddlValid at the SQL boundary.
func (t *storeTx) dynUpdate(ctx context.Context, ent *registry.Entity, aggID string, vals map[string]any) error {
	if !ddlValid(ent.Binding.Table) {
		return fmt.Errorf("fabriq: invalid dynamic table %q", ent.Binding.Table)
	}
	sets := make([]string, 0, len(vals))
	args := make([]any, 0, len(vals)+1)
	for _, c := range ent.Binding.Columns {
		if c == registry.ColumnID {
			continue
		}
		v, ok := vals[c]
		if !ok {
			continue
		}
		if !ddlValid(c) {
			return fmt.Errorf("fabriq: invalid column %q for %s", c, ent.Binding.Table)
		}
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s = $%d", quoteIdent(c), len(args)))
	}
	if len(sets) == 0 {
		return fmt.Errorf("fabriq: dynUpdate %s: no columns to update", ent.Binding.Table)
	}
	args = append(args, aggID)
	sql := fmt.Sprintf(`UPDATE %s SET %s WHERE id = $%d`,
		quoteIdent(ent.Binding.Table), strings.Join(sets, ", "), len(args))
	if _, err := t.ptx.NewRaw(sql, args...).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: update %s: %w", ent.Binding.Table, err)
	}
	return nil
}
