package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// migration0022DigestNode adds the digest_nodes table: the per-tenant
// context-distillation Merkle tree (domain.DigestNode). Columns mirror the grove
// tags on domain/digest_node.go; the registry-conformance test fails on drift.
var migration0022DigestNode = &migrate.Migration{
	Name:    "digest_node",
	Version: "202606190022",
	Comment: "context-distillation Merkle tree (domain.DigestNode) + RLS",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS digest_nodes (
				id           TEXT PRIMARY KEY,
				tenant_id    TEXT NOT NULL,
				version      BIGINT NOT NULL,
				level        INTEGER NOT NULL,
				kind         TEXT NOT NULL,
				scope_name   TEXT NOT NULL DEFAULT '',
				scope_id     TEXT NOT NULL DEFAULT '',
				source_id    TEXT NOT NULL DEFAULT '',
				source_kind  TEXT NOT NULL DEFAULT '',
				summary_hash TEXT NOT NULL DEFAULT '',
				content_hash TEXT NOT NULL DEFAULT '',
				sem_hash     TEXT NOT NULL DEFAULT '',
				child_ids    JSONB NOT NULL DEFAULT '[]'::jsonb,
				parent_ids   JSONB NOT NULL DEFAULT '[]'::jsonb,
				updated_at   BIGINT NOT NULL DEFAULT 0
			)`,
			`CREATE INDEX IF NOT EXISTS digest_nodes_tenant_idx ON digest_nodes (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS digest_nodes_level_idx ON digest_nodes (tenant_id, level)`,
			`CREATE INDEX IF NOT EXISTS digest_nodes_source_idx ON digest_nodes (tenant_id, source_kind, source_id)`,
			`CREATE INDEX IF NOT EXISTS digest_nodes_summary_hash_idx ON digest_nodes (summary_hash)`,
			`ALTER TABLE digest_nodes ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE digest_nodes FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON digest_nodes`,
			`CREATE POLICY tenant_isolation ON digest_nodes
				USING (tenant_id = current_setting('app.tenant_id', true))
				WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS digest_nodes`})
	},
}
