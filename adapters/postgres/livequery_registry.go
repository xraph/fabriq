package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/livequery"
)

// LiveSubscriptionRegistry is the Postgres-backed durable live subscription
// registry (fabriq_live_subscriptions) — the backbone the sharded matcher tier
// uses to recover subscriptions after failover. It is worker-plane (the table
// has no RLS); construct it on the worker/owner connection.
type LiveSubscriptionRegistry struct {
	db *pgdriver.PgDB
}

// NewLiveSubscriptionRegistry returns the durable registry over a Postgres
// handle (e.g. adapter.Driver()).
func NewLiveSubscriptionRegistry(db *pgdriver.PgDB) *LiveSubscriptionRegistry {
	return &LiveSubscriptionRegistry{db: db}
}

var _ livequery.SubscriptionRegistry = (*LiveSubscriptionRegistry)(nil)

func (r *LiveSubscriptionRegistry) Put(ctx context.Context, reg livequery.Registration) error {
	q, err := json.Marshal(reg.Query)
	if err != nil {
		return fmt.Errorf("fabriq: live registry marshal: %w", err)
	}
	if _, err := r.db.Exec(ctx, `
		INSERT INTO fabriq_live_subscriptions
			(sub_id, tenant_id, entity, mode, query, gateway_id, watermark, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (sub_id) DO UPDATE SET
			tenant_id = $2, entity = $3, mode = $4, query = $5,
			gateway_id = $6, watermark = $7, updated_at = now()`,
		reg.SubID, reg.TenantID, reg.Entity, int(reg.Mode), q, reg.GatewayID, reg.Watermark,
	); err != nil {
		return fmt.Errorf("fabriq: live registry put %s: %w", reg.SubID, err)
	}
	return nil
}

func (r *LiveSubscriptionRegistry) Delete(ctx context.Context, subID string) error {
	if _, err := r.db.Exec(ctx, `DELETE FROM fabriq_live_subscriptions WHERE sub_id = $1`, subID); err != nil {
		return fmt.Errorf("fabriq: live registry delete %s: %w", subID, err)
	}
	return nil
}

func (r *LiveSubscriptionRegistry) ByPartition(ctx context.Context, tenantID, entity string) ([]livequery.Registration, error) {
	return r.scan(ctx, `WHERE tenant_id = $1 AND entity = $2`, tenantID, entity)
}

func (r *LiveSubscriptionRegistry) ByGateway(ctx context.Context, gatewayID string) ([]livequery.Registration, error) {
	return r.scan(ctx, `WHERE gateway_id = $1`, gatewayID)
}

func (r *LiveSubscriptionRegistry) scan(ctx context.Context, where string, args ...any) ([]livequery.Registration, error) {
	rows, err := r.db.Query(ctx,
		`SELECT sub_id, tenant_id, entity, mode, query, gateway_id, watermark
		 FROM fabriq_live_subscriptions `+where+` ORDER BY created_at`, args...)
	if err != nil {
		return nil, fmt.Errorf("fabriq: live registry scan: %w", err)
	}
	defer rows.Close()
	var out []livequery.Registration
	for rows.Next() {
		var reg livequery.Registration
		var mode int
		var qb []byte
		if err := rows.Scan(&reg.SubID, &reg.TenantID, &reg.Entity, &mode, &qb, &reg.GatewayID, &reg.Watermark); err != nil {
			return nil, fmt.Errorf("fabriq: live registry scan row: %w", err)
		}
		reg.Mode = livequery.Mode(mode)
		if err := json.Unmarshal(qb, &reg.Query); err != nil {
			return nil, fmt.Errorf("fabriq: live registry decode query: %w", err)
		}
		out = append(out, reg)
	}
	return out, rows.Err()
}
