package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// CRDT document plane storage (the plane's implementation is deferred —
// see core/document/DESIGN.md; the tables exist from phase 1 so the seam
// is part of the schema contract):
//
//   - fabriq_crdt_updates: append-only update log, per-doc monotonic seq.
//   - fabriq_crdt_snapshots: compacted state up to last_seq; Compact folds
//     the log into the snapshot and trims seq <= last_seq.
//
// Tenant-data tables: RLS applies (sync sessions are tenant-stamped).
var migration0007CRDTUpdates = &migrate.Migration{
	Name:    "crdt_updates",
	Version: "202606120007",
	Comment: "CRDT document plane: append-only update log + snapshots",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fabriq_crdt_updates (
				doc_id      TEXT NOT NULL,
				seq         BIGINT GENERATED ALWAYS AS IDENTITY,
				tenant_id   TEXT NOT NULL,
				update_data BYTEA NOT NULL,
				at          TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (doc_id, seq)
			)`,
			`CREATE TABLE IF NOT EXISTS fabriq_crdt_snapshots (
				doc_id    TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				snapshot  BYTEA NOT NULL,
				last_seq  BIGINT NOT NULL,
				at        TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
		}
		for _, table := range []string{"fabriq_crdt_updates", "fabriq_crdt_snapshots"} {
			stmts = append(stmts,
				`ALTER TABLE `+table+` ENABLE ROW LEVEL SECURITY`,
				`ALTER TABLE `+table+` FORCE ROW LEVEL SECURITY`,
				`DROP POLICY IF EXISTS tenant_isolation ON `+table,
				`CREATE POLICY tenant_isolation ON `+table+`
					USING (tenant_id = current_setting('app.tenant_id', true))
					WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
			)
		}
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_crdt_snapshots`,
			`DROP TABLE IF EXISTS fabriq_crdt_updates`,
		})
	},
}
