package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Document-plane bookkeeping + the TWINOS demo document entity:
//
//   - fabriq_crdt_docs: one row per live document — entity binding (doc
//     ids are "<entity>/<ulid>"), materialization watermark, quiet-window
//     activity timestamp, and the post-merge-validation flag. WORKER-PLANE
//     table like fabriq_outbox: no RLS (the materializer scans across
//     tenants; it holds no user content — document content lives in
//     fabriq_crdt_updates/snapshots, which keep RLS and are only reached
//     through tenant-stamped transactions).
//   - pages: the KindDocument demo entity (collaborative page-builder
//     documents); rows are written ONLY by materialization. RLS applies.
var migration0008CRDTDocs = &migrate.Migration{
	Name:    "crdt_docs",
	Version: "202606120008",
	Comment: "document registry/bookkeeping + pages demo entity",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := make([]string, 0, 12)
		stmts = append(stmts,
			`CREATE TABLE IF NOT EXISTS fabriq_crdt_docs (
				doc_id      TEXT PRIMARY KEY,
				tenant_id   TEXT NOT NULL,
				entity      TEXT NOT NULL,
				last_seq    BIGINT NOT NULL DEFAULT 0,
				last_seq_materialized BIGINT NOT NULL DEFAULT 0,
				flagged     BOOLEAN NOT NULL DEFAULT FALSE,
				flag_reason TEXT NOT NULL DEFAULT '',
				updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS fabriq_crdt_docs_tenant_idx ON fabriq_crdt_docs (tenant_id, updated_at)`,
			`CREATE TABLE IF NOT EXISTS pages (
				id        TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				version   BIGINT NOT NULL,
				title     TEXT NOT NULL DEFAULT '',
				body      TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS pages_tenant_idx ON pages (tenant_id)`,
		)
		for _, table := range []string{"pages"} {
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
			`DROP TABLE IF EXISTS pages`,
			`DROP TABLE IF EXISTS fabriq_crdt_docs`,
		})
	},
}
