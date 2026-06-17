package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

// ScopeAwareTenantPolicy returns the SQL to (re)create the tenant_isolation
// policy with the soft secondary-scope predicate for a table that has a nullable
// scope_id column. Tenant stays the hard boundary; scope is soft: an unscoped
// read (app.scope_id=”) sees all rows in the tenant; a scoped read sees its
// scope plus shared (NULL-scope) rows. Consumers (kgkit/twinos) reuse this for
// their own entity tables that adopt scope_id.
func ScopeAwareTenantPolicy(table string) []string {
	return []string{
		fmt.Sprintf(`ALTER TABLE %s ENABLE ROW LEVEL SECURITY`, table),
		fmt.Sprintf(`ALTER TABLE %s FORCE ROW LEVEL SECURITY`, table),
		fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, table),
		fmt.Sprintf(`CREATE POLICY tenant_isolation ON %s
			USING ( tenant_id = current_setting('app.tenant_id', true)
				AND ( current_setting('app.scope_id', true) = ''
					OR scope_id IS NULL
					OR scope_id = current_setting('app.scope_id', true) ) )
			WITH CHECK ( tenant_id = current_setting('app.tenant_id', true) )`, table),
	}
}

// tableExists reports whether the named table exists in the public schema.
func tableExists(ctx context.Context, exec migrate.Executor, name string) (bool, error) {
	rows, err := exec.Query(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
		name,
	)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
	}
	return n > 0, rows.Err()
}

// scopeTables are the fabriq-managed RLS tables that gain a soft scope filter.
// sites/assets/tags (0004) are omitted: they are core entity tables whose rows
// never carry a secondary scope at the fabriq layer — the old tenant_isolation
// policy (tenant_id = app.tenant_id) continues to work for them because their
// scope_id will always be NULL. If a consumer wants scope on those tables they
// should call ScopeAwareTenantPolicy directly after adding scope_id themselves.
// fabriq_embeddings and fabriq_geometries are fabriq-managed optional tables
// (pgvector/PostGIS) that are natural scope boundaries for vector/spatial data.
var scopeTables = []string{"fabriq_embeddings", "fabriq_geometries"}

var migration0012Scope = &migrate.Migration{
	Name:    "native_scope",
	Version: "202606160012",
	Comment: "secondary scope_id column + soft RLS predicate on fabriq-managed tables",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		for _, t := range scopeTables {
			exists, err := tableExists(ctx, exec, t)
			if err != nil {
				return err
			}
			if !exists {
				continue // pgvector/postgis table absent on this DB — skip
			}
			stmts := []string{fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS scope_id TEXT`, t)}
			stmts = append(stmts, ScopeAwareTenantPolicy(t)...)
			if err := execAll(ctx, exec, stmts); err != nil {
				return err
			}
		}
		return nil
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		for _, t := range scopeTables {
			exists, err := tableExists(ctx, exec, t)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			stmts := []string{
				fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, t),
				fmt.Sprintf(`CREATE POLICY tenant_isolation ON %s
					USING ( tenant_id = current_setting('app.tenant_id', true) )
					WITH CHECK ( tenant_id = current_setting('app.tenant_id', true) )`, t),
				fmt.Sprintf(`ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS scope_id`, t),
			}
			if err := execAll(ctx, exec, stmts); err != nil {
				return err
			}
		}
		return nil
	},
}
