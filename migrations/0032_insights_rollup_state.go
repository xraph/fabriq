package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0032InsightsRollupState = &migrate.Migration{
	Name:    "insights_rollup_state",
	Version: "202607130032",
	Comment: "per-tenant customer-facing analytics: rollup watermark state (RLS)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fabriq_insights_rollup_state (
				tenant_id        TEXT NOT NULL,
				scope_id         TEXT,
				metric           TEXT NOT NULL,
				watermark_bucket TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (tenant_id, metric)
			)`,
		}
		stmts = append(stmts, ScopeAwareTenantPolicy("fabriq_insights_rollup_state")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_insights_rollup_state`,
		})
	},
}
