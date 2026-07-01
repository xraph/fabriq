package postgres

import (
	"context"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/projection"
)

// StateRepo is the Postgres-backed projection bookkeeping (worker-plane
// tables, no RLS — consumers and the reconciler are cross-tenant by
// design). It implements projection.StateRepo.
type StateRepo struct {
	pg *pgdriver.PgDB
}

var _ projection.StateRepo = (*StateRepo)(nil)

// AppliedVersion implements projection.StateReader.
func (r *StateRepo) AppliedVersion(ctx context.Context, tenantID, proj, aggregate, aggID string) (int64, error) {
	row := r.pg.QueryRow(ctx, `SELECT COALESCE((
			SELECT version FROM fabriq_projection_applied
			WHERE tenant_id = $1 AND projection = $2 AND aggregate = $3 AND agg_id = $4
		), 0)`, tenantID, proj, aggregate, aggID)
	var v int64
	if err := row.Scan(&v); err != nil {
		return 0, fmt.Errorf("fabriq: applied version: %w", err)
	}
	return v, nil
}

// SetApplied records a projection apply; the watermark never regresses.
func (r *StateRepo) SetApplied(ctx context.Context, tenantID, proj, aggregate, aggID string, version int64) error {
	_, err := r.pg.Exec(ctx, `INSERT INTO fabriq_projection_applied
			(tenant_id, projection, aggregate, agg_id, version)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tenant_id, projection, aggregate, agg_id)
			DO UPDATE SET version = GREATEST(fabriq_projection_applied.version, EXCLUDED.version), updated_at = now()`,
		tenantID, proj, aggregate, aggID, version)
	if err != nil {
		return fmt.Errorf("fabriq: set applied: %w", err)
	}
	return nil
}

// Get implements projection.StateRepo.
func (r *StateRepo) Get(ctx context.Context, tenantID, proj string) (projection.State, error) {
	row := r.pg.QueryRow(ctx, `SELECT tenant_id, projection, model_version, event_version, status, target_name
			FROM fabriq_projection_state WHERE tenant_id = $1 AND projection = $2`, tenantID, proj)
	var s projection.State
	if err := row.Scan(&s.TenantID, &s.Projection, &s.ModelVersion, &s.EventVersion, &s.Status, &s.TargetName); err != nil {
		if isNoRows(err) {
			return projection.State{TenantID: tenantID, Projection: proj, ModelVersion: 1, Status: "live"}, nil
		}
		return projection.State{}, translatePg("get", "", "", fmt.Errorf("fabriq: projection state: %w", err))
	}
	return s, nil
}

// Tenants lists every tenant that has ever emitted an event (worker-plane
// discovery for rebuild --all-tenants and the reconciler; the outbox has
// no RLS, so this sees across tenants by design).
func (r *StateRepo) Tenants(ctx context.Context) ([]string, error) {
	rows, err := r.pg.Query(ctx, `SELECT DISTINCT tenant_id FROM fabriq_outbox ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("fabriq: list tenants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Upsert implements projection.StateRepo.
func (r *StateRepo) Upsert(ctx context.Context, s projection.State) error {
	_, err := r.pg.Exec(ctx, `INSERT INTO fabriq_projection_state
			(tenant_id, projection, model_version, event_version, status, target_name, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, now())
			ON CONFLICT (tenant_id, projection)
			DO UPDATE SET model_version = EXCLUDED.model_version, event_version = EXCLUDED.event_version,
				status = EXCLUDED.status, target_name = EXCLUDED.target_name, updated_at = now()`,
		s.TenantID, s.Projection, s.ModelVersion, s.EventVersion, s.Status, s.TargetName)
	if err != nil {
		return fmt.Errorf("fabriq: upsert projection state: %w", err)
	}
	return nil
}
