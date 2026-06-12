package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

// rlsTables are the tenant-data tables under row-level security. FORCE
// applies the policy to the table owner too, so the application role is
// constrained even when it owns the schema. The policy keys on
// current_setting('app.tenant_id', true), which the postgres adapter sets
// with SET LOCAL inside every tenant transaction; outside a stamped
// transaction the setting is NULL and no rows are visible.
//
// tag_readings is deliberately ABSENT: TimescaleDB's columnstore
// (compression) cannot coexist with row security ("columnstore cannot be
// used on table with row security"), and compressed telemetry is the
// reason Timescale is here at all. Readings tenancy is enforced
// structurally by the TSQuerier (every query carries tenant_id) plus the
// adapter's raw-SQL guard — see docs/decisions/0006-timescale-rls.md.
var rlsTables = []string{"sites", "assets", "tags"}

var migration0004RLSPolicies = &migrate.Migration{
	Name:    "rls_policies",
	Version: "202606120004",
	Comment: "row-level security: tenant isolation enforced by the database",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		for _, table := range rlsTables {
			stmts := []string{
				fmt.Sprintf(`ALTER TABLE %s ENABLE ROW LEVEL SECURITY`, table),
				fmt.Sprintf(`ALTER TABLE %s FORCE ROW LEVEL SECURITY`, table),
				fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, table),
				fmt.Sprintf(`CREATE POLICY tenant_isolation ON %s
					USING (tenant_id = current_setting('app.tenant_id', true))
					WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`, table),
			}
			if err := execAll(ctx, exec, stmts); err != nil {
				return err
			}
		}
		return nil
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		for _, table := range rlsTables {
			stmts := []string{
				fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, table),
				fmt.Sprintf(`ALTER TABLE %s NO FORCE ROW LEVEL SECURITY`, table),
				fmt.Sprintf(`ALTER TABLE %s DISABLE ROW LEVEL SECURITY`, table),
			}
			if err := execAll(ctx, exec, stmts); err != nil {
				return err
			}
		}
		return nil
	},
}
