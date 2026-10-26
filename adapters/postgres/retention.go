package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

var _ command.ErasureTx = (*storeTx)(nil)

func lockAggregate(ctx context.Context, tx *pgdriver.PgTx, tid, entity, id string) error {
	identity, err := json.Marshal([]string{tid, entity, id})
	if err != nil {
		return err
	}
	_, err = tx.NewRaw(`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, string(identity)).Exec(ctx)
	return err
}

func fenceAggregate(ctx context.Context, tx *pgdriver.PgTx, entity, id string) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	if err := lockAggregate(ctx, tx, tid, entity, id); err != nil {
		return err
	}
	var rows []struct {
		Present bool `grove:"present"`
	}
	if err := tx.NewRaw(`SELECT EXISTS(SELECT 1 FROM fabriq_aggregate_tombstones WHERE tenant_id=$1 AND entity=$2 AND agg_id=$3) AS present`, tid, entity, id).Scan(ctx, &rows); err != nil {
		return err
	}
	if len(rows) != 1 {
		return fabriqerr.ErrStoreNotConfigured
	}
	if rows[0].Present {
		return fabriqerr.ErrAggregateErased
	}
	return nil
}

// FenceAggregate serializes every ordinary command with irreversible retention.
func (t *storeTx) FenceAggregate(ctx context.Context, ent *registry.Entity, id string) error {
	return fenceAggregate(ctx, t.ptx, ent.Spec.Name, id)
}

func retentionConflict() error {
	return fabriqerr.New(fabriqerr.CodeVersionConflict, "Aggregate retention target changed.")
}

var localRetentionTargets = []string{"aggregate_row", "command_outbox", "embedding", "spatial"}

// EraseAggregate erases only the exact authorized scope and local payload targets.
// The temporary tenant-only read setting detects hidden conflicting scope rows;
// every deletion still includes the exact scope and the transaction cannot expose
// those rows to the caller. Relay row locks join any in-flight local publication.
func (t *storeTx) EraseAggregate(ctx context.Context, ent *registry.Entity, r command.ErasureRequest, g command.ErasureGrant, now time.Time) (result command.ErasureReceipt, returnErr error) {
	var zero command.ErasureReceipt
	if err := r.Validate(ctx); err != nil {
		return zero, err
	}
	if ent == nil || ent.Spec.Name != r.Entity || ent.Spec.Kind != registry.KindAggregate || !ddlValid(ent.Binding.Table) || now.IsZero() {
		return zero, retentionConflict()
	}
	tid, tenantErr := tenant.Require(ctx)
	if tenantErr != nil {
		return zero, tenantErr
	}
	if err := lockAggregate(ctx, t.ptx, tid, r.Entity, r.AggregateID); err != nil {
		return zero, err
	}
	var retained []struct {
		Receipt string `grove:"receipt"`
	}
	if err := t.ptx.NewRaw(`SELECT receipt::text AS receipt FROM fabriq_aggregate_tombstones WHERE tenant_id=$1 AND entity=$2 AND agg_id=$3`, tid, r.Entity, r.AggregateID).Scan(ctx, &retained); err != nil {
		return zero, err
	}
	if len(retained) > 0 {
		var receipt command.ErasureReceipt
		if len(retained) != 1 || len(retained[0].Receipt) > 16<<10 || json.Unmarshal([]byte(retained[0].Receipt), &receipt) != nil || receipt.TenantID != tid || receipt.Request != r || !receipt.LocalPayloadsErased || !reflect.DeepEqual(receipt.LocalTargets, localRetentionTargets) {
			return zero, retentionConflict()
		}
		return receipt, nil
	}
	if _, err := t.ptx.NewRaw(`SELECT set_config('app.scope_id','',true)`).Exec(ctx); err != nil {
		return zero, err
	}
	defer func() {
		if _, restoreErr := t.ptx.NewRaw(`SELECT set_config('app.scope_id',$1,true)`, r.ScopeID).Exec(ctx); restoreErr != nil {
			result = zero
			returnErr = errors.Join(returnErr, restoreErr)
		}
	}()
	scopeExpr := `''::text`
	if ent.Binding.HasColumn(registry.ColumnScope) {
		scopeExpr = `COALESCE(scope_id,'')`
	}
	var active []struct {
		Version int64  `grove:"version"`
		Scope   string `grove:"scope"`
	}
	sql := fmt.Sprintf(`SELECT version,%s AS scope FROM %s WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, scopeExpr, quoteIdent(ent.Binding.Table))
	if err := t.ptx.NewRaw(sql, tid, r.AggregateID).Scan(ctx, &active); err != nil {
		return zero, err
	}
	version := int64(0)
	if len(active) > 1 {
		return zero, retentionConflict()
	}
	if len(active) == 1 {
		version = active[0].Version
		if active[0].Scope != r.ScopeID {
			return zero, retentionConflict()
		}
	}
	if version != r.ExpectedVersion {
		return zero, retentionConflict()
	}
	var history []struct {
		Version int64 `grove:"version"`
		Foreign bool  `grove:"foreign_scope"`
	}
	if err := t.ptx.NewRaw(`SELECT COALESCE(MAX(version),0) AS version,COALESCE(bool_or(payload<>'{}'::jsonb AND COALESCE(payload->>'scope_id','')<>$4),false) AS foreign_scope FROM fabriq_outbox WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, tid, r.Entity, r.AggregateID, r.ScopeID).Scan(ctx, &history); err != nil {
		return zero, err
	}
	if len(history) != 1 || history[0].Foreign {
		return zero, retentionConflict()
	}
	for _, table := range []string{"fabriq_embeddings", "fabriq_geometries"} {
		if err := t.eraseRetentionProjection(ctx, table, tid, r); err != nil {
			return zero, err
		}
	}
	sql = fmt.Sprintf(`DELETE FROM %s WHERE tenant_id=$1 AND id=$2 AND %s=$3`, quoteIdent(ent.Binding.Table), scopeExpr)
	if _, err := t.ptx.NewRaw(sql, tid, r.AggregateID, r.ScopeID).Exec(ctx); err != nil {
		return zero, err
	}
	var counts []struct {
		Count int64 `grove:"count"`
	}
	if err := t.ptx.NewRaw(`WITH erased AS (UPDATE fabriq_outbox SET payload='{}'::jsonb,traceparent='',erased_at=$4 WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3 AND (payload='{}'::jsonb OR COALESCE(payload->>'scope_id','')=$5) RETURNING 1) SELECT count(*) AS count FROM erased`, tid, r.Entity, r.AggregateID, now.UTC(), r.ScopeID).Scan(ctx, &counts); err != nil {
		return zero, err
	}
	if len(counts) != 1 {
		return zero, retentionConflict()
	}
	receipt := command.ErasureReceipt{TenantID: tid, Request: r, Actor: g.Actor, AuthorityRevision: g.AuthorityRevision, LastVersion: max(version, history[0].Version), ErasedAt: now.UTC(), LocalPayloadsErased: true, LocalTargets: append([]string(nil), localRetentionTargets...), OutboxRowsRedacted: counts[0].Count}
	for _, target := range []string{"streams", "graph", "search", "cache", "live_subscribers", "backups"} {
		receipt.Downstream = append(receipt.Downstream, command.CleanupStatus{Target: target, State: "unconfirmed"})
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return zero, err
	}
	if _, err := t.ptx.NewRaw(`INSERT INTO fabriq_aggregate_tombstones(tenant_id,entity,agg_id,scope_id,receipt) VALUES($1,$2,$3,$4,$5::jsonb)`, tid, r.Entity, r.AggregateID, r.ScopeID, string(data)).Exec(ctx); err != nil {
		return zero, err
	}
	return receipt, ctx.Err()
}

func (t *storeTx) eraseRetentionProjection(ctx context.Context, table, tid string, r command.ErasureRequest) error {
	var tables []struct {
		Present bool `grove:"present"`
	}
	if err := t.ptx.NewRaw(`SELECT to_regclass($1) IS NOT NULL AS present`, table).Scan(ctx, &tables); err != nil {
		return err
	}
	if len(tables) != 1 {
		return retentionConflict()
	}
	if !tables[0].Present {
		return nil
	}
	var scopes []struct {
		Scope string `grove:"scope"`
	}
	sql := fmt.Sprintf(`SELECT COALESCE(scope_id,'') AS scope FROM %s WHERE tenant_id=$1 AND entity=$2 AND id=$3 FOR UPDATE`, quoteIdent(table))
	if err := t.ptx.NewRaw(sql, tid, r.Entity, r.AggregateID).Scan(ctx, &scopes); err != nil {
		return err
	}
	for _, row := range scopes {
		if row.Scope != r.ScopeID {
			return retentionConflict()
		}
	}
	sql = fmt.Sprintf(`DELETE FROM %s WHERE tenant_id=$1 AND entity=$2 AND id=$3 AND COALESCE(scope_id,'')=$4`, quoteIdent(table))
	_, err := t.ptx.NewRaw(sql, tid, r.Entity, r.AggregateID, r.ScopeID).Exec(ctx)
	return err
}
