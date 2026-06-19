package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0018FsPermission = &migrate.Migration{
	Name:    "fs_permission",
	Version: "202606190018",
	Comment: "fs_permission ACL grants (FK fs_node ON DELETE CASCADE)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fs_permissions (
				id             TEXT PRIMARY KEY,
				tenant_id      TEXT NOT NULL,
				scope_id       TEXT,
				version        BIGINT NOT NULL,
				node_id        TEXT NOT NULL REFERENCES fs_nodes(id) ON DELETE CASCADE,
				principal_type TEXT NOT NULL,
				principal_id   TEXT NOT NULL,
				permission     TEXT NOT NULL,
				granted_by     TEXT NOT NULL DEFAULT '',
				created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS fs_permissions_node_idx ON fs_permissions (tenant_id, node_id)`,
			`CREATE INDEX IF NOT EXISTS fs_permissions_principal_idx ON fs_permissions (tenant_id, principal_type, principal_id)`,
		}
		stmts = append(stmts, ScopeAwareTenantPolicy("fs_permissions")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS fs_permissions`})
	},
}
