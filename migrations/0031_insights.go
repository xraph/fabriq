package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0031Insights = &migrate.Migration{
	Name:    "insights",
	Version: "202607100031",
	Comment: "per-tenant customer-facing analytics: events + projected facts (RLS)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		eventsPolicy := ScopeAwareTenantPolicy("fabriq_insights_events")
		factsPolicy := ScopeAwareTenantPolicy("fabriq_insights_facts")
		stmts := make([]string, 0, 5+len(eventsPolicy)+len(factsPolicy))
		stmts = append(stmts,
			`CREATE TABLE IF NOT EXISTS fabriq_insights_events (
				id         BIGSERIAL PRIMARY KEY,
				tenant_id  TEXT NOT NULL,
				scope_id   TEXT,
				name       TEXT NOT NULL,
				at         TIMESTAMPTZ NOT NULL,
				props      JSONB NOT NULL DEFAULT '{}'::jsonb,
				dedup_key  TEXT
			)`,
			`CREATE INDEX IF NOT EXISTS fabriq_insights_events_tenant_name_at_idx
				ON fabriq_insights_events (tenant_id, name, at)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS fabriq_insights_events_dedup_idx
				ON fabriq_insights_events (tenant_id, dedup_key) WHERE dedup_key IS NOT NULL`,
			`CREATE TABLE IF NOT EXISTS fabriq_insights_facts (
				tenant_id  TEXT NOT NULL,
				scope_id   TEXT,
				entity     TEXT NOT NULL,
				agg_id     TEXT NOT NULL,
				version    BIGINT NOT NULL,
				payload    JSONB NOT NULL,
				at         TIMESTAMPTZ NOT NULL,
				deleted    BOOLEAN NOT NULL DEFAULT false,
				PRIMARY KEY (tenant_id, entity, agg_id)
			)`,
			`CREATE INDEX IF NOT EXISTS fabriq_insights_facts_tenant_entity_idx
				ON fabriq_insights_facts (tenant_id, entity)`,
		)
		stmts = append(stmts, eventsPolicy...)
		stmts = append(stmts, factsPolicy...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_insights_events`,
			`DROP TABLE IF EXISTS fabriq_insights_facts`,
		})
	},
}
