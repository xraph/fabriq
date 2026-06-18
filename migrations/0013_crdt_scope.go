package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

// crdtScopeRLSTables are the CRDT content tables (created in 0007) that carry
// user document content and already enforce tenant_isolation RLS. They gain a
// nullable scope_id column and the soft scope-aware policy so Sync inherits the
// secondary-scope filter exactly like fabriq_embeddings / fabriq_geometries:
// an unscoped read sees every doc in the tenant; a scoped read sees its own
// scope plus shared (NULL-scope) docs.
var crdtScopeRLSTables = []string{"fabriq_crdt_updates", "fabriq_crdt_snapshots"}

// crdtScopeColumnOnlyTables are CRDT worker-plane tables that gain scope_id but
// keep NO RLS. fabriq_crdt_docs is bookkeeping the materializer scans across
// tenants (like fabriq_outbox); it holds no document content — content lives in
// the RLS'd update log / snapshots — so its scope_id is informational and the
// boundary is enforced on the content tables above.
var crdtScopeColumnOnlyTables = []string{"fabriq_crdt_docs"}

var migration0013CRDTScope = &migrate.Migration{
	Name:    "crdt_native_scope",
	Version: "202606160013",
	Comment: "secondary scope_id column + soft RLS predicate on the CRDT document-plane tables",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		// RLS content tables: add scope_id + upgrade the policy to scope-aware.
		for _, t := range crdtScopeRLSTables {
			exists, err := tableExists(ctx, exec, t)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			policy := ScopeAwareTenantPolicy(t)
			stmts := make([]string, 0, 1+len(policy))
			stmts = append(stmts, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS scope_id TEXT`, t))
			stmts = append(stmts, policy...)
			if err := execAll(ctx, exec, stmts); err != nil {
				return err
			}
		}
		// Worker-plane bookkeeping: add scope_id only — no RLS.
		for _, t := range crdtScopeColumnOnlyTables {
			exists, err := tableExists(ctx, exec, t)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			if err := execAll(ctx, exec, []string{
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS scope_id TEXT`, t),
			}); err != nil {
				return err
			}
		}
		return nil
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		// RLS content tables: restore the tenant-only policy (the 0007 shape),
		// then drop scope_id. Order matters: the policy must not reference a
		// column that is about to be dropped.
		for _, t := range crdtScopeRLSTables {
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
		// Worker-plane bookkeeping: drop scope_id only.
		for _, t := range crdtScopeColumnOnlyTables {
			exists, err := tableExists(ctx, exec, t)
			if err != nil {
				return err
			}
			if !exists {
				continue
			}
			if err := execAll(ctx, exec, []string{
				fmt.Sprintf(`ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS scope_id`, t),
			}); err != nil {
				return err
			}
		}
		return nil
	},
}
