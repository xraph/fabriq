package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0020FsBookmark = &migrate.Migration{
	Name:    "fs_bookmark",
	Version: "202606190020",
	Comment: "fs_bookmark user favourites (FK fs_node ON DELETE CASCADE)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := append([]string{
			`CREATE TABLE IF NOT EXISTS fs_bookmarks (
				id         TEXT PRIMARY KEY,
				tenant_id  TEXT NOT NULL,
				scope_id   TEXT,
				version    BIGINT NOT NULL,
				user_id    TEXT NOT NULL,
				node_id    TEXT NOT NULL REFERENCES fs_nodes(id) ON DELETE CASCADE,
				sort_order INTEGER NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS fs_bookmarks_uniq ON fs_bookmarks (tenant_id, user_id, node_id)`,
			`CREATE INDEX IF NOT EXISTS fs_bookmarks_user_idx ON fs_bookmarks (tenant_id, user_id)`,
		}, ScopeAwareTenantPolicy("fs_bookmarks")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS fs_bookmarks`})
	},
}
