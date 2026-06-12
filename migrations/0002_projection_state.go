package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Projection bookkeeping. fabriq_projection_state is the spec'd
// per-(tenant, projection) row: live pointer (blue-green target_name),
// model_version, stream position, status. fabriq_projection_applied adds
// per-aggregate applied versions so WaitForProjection can answer
// read-your-writes questions without engine-specific lookups.
//
// Worker-plane tables: no RLS (consumers and the reconciler work across
// tenants by design).
var migration0002ProjectionState = &migrate.Migration{
	Name:    "projection_state",
	Version: "202606120002",
	Comment: "projection bookkeeping: state + per-aggregate applied versions",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_projection_state (
				tenant_id     TEXT NOT NULL,
				projection    TEXT NOT NULL,
				model_version INT NOT NULL DEFAULT 1,
				event_version TEXT NOT NULL DEFAULT '',
				status        TEXT NOT NULL DEFAULT 'live',
				target_name   TEXT NOT NULL DEFAULT '',
				updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (tenant_id, projection)
			)`,
			`CREATE TABLE IF NOT EXISTS fabriq_projection_applied (
				tenant_id  TEXT NOT NULL,
				projection TEXT NOT NULL,
				aggregate  TEXT NOT NULL,
				agg_id     TEXT NOT NULL,
				version    BIGINT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (tenant_id, projection, aggregate, agg_id)
			)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_projection_applied`,
			`DROP TABLE IF EXISTS fabriq_projection_state`,
		})
	},
}
