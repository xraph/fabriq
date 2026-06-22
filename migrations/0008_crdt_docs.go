package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Document-plane bookkeeping:
//
//   - fabriq_crdt_docs: one row per live document — entity binding (doc
//     ids are "<entity>/<ulid>"), materialization watermark, quiet-window
//     activity timestamp, and the post-merge-validation flag. WORKER-PLANE
//     table like fabriq_outbox: no RLS (the materializer scans across
//     tenants; it holds no user content — document content lives in
//     fabriq_crdt_updates/snapshots, which keep RLS and are only reached
//     through tenant-stamped transactions).
//
// The materialization TARGET tables are application-defined entities, NOT core
// schema — fabriq must never create a generically-named table (e.g. "pages")
// in a host's database, where it would collide with the app's own tables. The
// bundled demo entity's DDL therefore lives with its model in package domain
// (domain.PagesDDL), applied only by examples and the document-plane tests.
var migration0008CRDTDocs = &migrate.Migration{
	Name:    "crdt_docs",
	Version: "202606120008",
	Comment: "document registry/bookkeeping",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
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
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_crdt_docs`,
		})
	},
}
