package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0019FsShare = &migrate.Migration{
	Name:    "fs_share",
	Version: "202606190019",
	Comment: "fs_share share-link records (FK fs_node ON DELETE CASCADE)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS fs_shares (
				id             TEXT PRIMARY KEY,
				tenant_id      TEXT NOT NULL,
				scope_id       TEXT,
				version        BIGINT NOT NULL,
				node_id        TEXT NOT NULL REFERENCES fs_nodes(id) ON DELETE CASCADE,
				token          TEXT NOT NULL,
				permission     TEXT NOT NULL DEFAULT 'read',
				expires_at     TIMESTAMPTZ,
				max_downloads  INTEGER,
				download_count INTEGER NOT NULL DEFAULT 0,
				password_hash  TEXT NOT NULL DEFAULT '',
				created_by     TEXT NOT NULL DEFAULT '',
				created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS fs_shares_token_uniq ON fs_shares (tenant_id, token)`,
			`CREATE INDEX IF NOT EXISTS fs_shares_node_idx ON fs_shares (tenant_id, node_id)`,
		}
		stmts = append(stmts, ScopeAwareTenantPolicy("fs_shares")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS fs_shares`})
	},
}
