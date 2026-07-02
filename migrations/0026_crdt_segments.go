package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_crdt_segments indexes offloaded CRDT history: each row points at one
// immutable blob segment holding the sealed updates for a contiguous seq range
// [seq_lo, seq_hi] of a document. Compact seals the trimmed log tail into a
// segment (blob Put) and records the row here before deleting the rows.
//
// Content table: it carries the map from tenant docs to their history bytes, so
// it gets the same scope-aware tenant RLS as fabriq_crdt_updates / _snapshots.
var migration0026CRDTSegments = &migrate.Migration{
	Name:    "crdt_segments",
	Version: "202607010026",
	Comment: "CRDT history offload: segment index (seq range -> blob key)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fabriq_crdt_segments (
				doc_id       TEXT NOT NULL,
				seg_seq      BIGINT GENERATED ALWAYS AS IDENTITY,
				tenant_id    TEXT NOT NULL,
				seq_lo       BIGINT NOT NULL,
				seq_hi       BIGINT NOT NULL,
				blob_key     TEXT NOT NULL,
				byte_size    BIGINT NOT NULL,
				update_count BIGINT NOT NULL,
				scope_id     TEXT,
				at           TIMESTAMPTZ NOT NULL DEFAULT now(),
				PRIMARY KEY (doc_id, seg_seq)
			)`,
			`CREATE INDEX IF NOT EXISTS fabriq_crdt_segments_range_idx
				ON fabriq_crdt_segments (doc_id, seq_lo, seq_hi)`,
			`ALTER TABLE fabriq_crdt_segments ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE fabriq_crdt_segments FORCE ROW LEVEL SECURITY`,
		}
		// Scope-aware tenant policy, same shape as migration 0013 applies to the
		// other CRDT content tables.
		stmts = append(stmts, ScopeAwareTenantPolicy("fabriq_crdt_segments")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_crdt_segments`,
		})
	},
}
