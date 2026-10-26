package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

var migration0036AggregateRetention = &migrate.Migration{
	Name: "aggregate_retention", Version: "202610260036", Comment: "irreversible aggregate tombstones and local outbox redaction",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fabriq_outbox ADD COLUMN IF NOT EXISTS erased_at TIMESTAMPTZ`,
			`CREATE TABLE IF NOT EXISTS fabriq_aggregate_tombstones (
			tenant_id TEXT NOT NULL, entity TEXT NOT NULL, agg_id TEXT NOT NULL,
			scope_id TEXT NOT NULL, receipt JSONB NOT NULL,
			PRIMARY KEY (tenant_id,entity,agg_id))`,
			`ALTER TABLE fabriq_aggregate_tombstones ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE fabriq_aggregate_tombstones FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON fabriq_aggregate_tombstones`,
			`CREATE POLICY tenant_isolation ON fabriq_aggregate_tombstones
			USING (tenant_id=current_setting('app.tenant_id',true))
			WITH CHECK (tenant_id=current_setting('app.tenant_id',true))`,
			`CREATE OR REPLACE FUNCTION fabriq_reject_tombstone_change() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'aggregate tombstones are immutable' USING ERRCODE='23000'; END $$`,
			`DROP TRIGGER IF EXISTS immutable_aggregate_tombstone ON fabriq_aggregate_tombstones`,
			`CREATE TRIGGER immutable_aggregate_tombstone BEFORE UPDATE OR DELETE ON fabriq_aggregate_tombstones
			FOR EACH ROW EXECUTE FUNCTION fabriq_reject_tombstone_change()`,
		})
	},
	Down: func(context.Context, migrate.Executor) error {
		return fmt.Errorf("fabriq: aggregate retention cannot be rolled back without discarding permanent identity fences")
	},
}
