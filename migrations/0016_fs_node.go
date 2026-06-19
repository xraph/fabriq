package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0016FsNode = &migrate.Migration{
	Name:    "fs_node",
	Version: "202606180016",
	Comment: "fs_node filesystem tree: adjacency + materialized path + scope RLS",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fs_nodes (
				id           TEXT PRIMARY KEY,
				tenant_id    TEXT NOT NULL,
				scope_id     TEXT,
				version      BIGINT NOT NULL,
				parent_id    TEXT NOT NULL DEFAULT '',
				name         TEXT NOT NULL,
				path         TEXT NOT NULL DEFAULT '',
				node_type    TEXT NOT NULL,
				blob_id      TEXT NOT NULL DEFAULT '',
				size         BIGINT NOT NULL DEFAULT 0,
				content_type TEXT NOT NULL DEFAULT '',
				checksum     TEXT NOT NULL DEFAULT '',
				is_locked    BOOLEAN NOT NULL DEFAULT FALSE,
				locked_by    TEXT NOT NULL DEFAULT '',
				metadata     JSONB NOT NULL DEFAULT '{}',
				deleted_at   TIMESTAMPTZ,
				deleted_by   TEXT NOT NULL DEFAULT '',
				created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS fs_nodes_tenant_idx ON fs_nodes (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS fs_nodes_children_idx ON fs_nodes (tenant_id, parent_id) WHERE deleted_at IS NULL`,
			`CREATE UNIQUE INDEX IF NOT EXISTS fs_nodes_sibling_uniq ON fs_nodes (tenant_id, parent_id, name) WHERE deleted_at IS NULL`,
			`CREATE INDEX IF NOT EXISTS fs_nodes_path_idx ON fs_nodes (tenant_id, path text_pattern_ops)`,
		}
		stmts = append(stmts, ScopeAwareTenantPolicy("fs_nodes")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS fs_nodes`})
	},
}
