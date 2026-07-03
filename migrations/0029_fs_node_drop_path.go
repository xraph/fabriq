package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0029FsNodeDropPath = &migrate.Migration{
	Name:    "fs_node_drop_path",
	Version: "202607030029",
	Comment: "fs_node pure adjacency: drop materialized path column + index (paths derive from parent_id at read time)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP INDEX IF EXISTS fs_nodes_path_idx`,
			`ALTER TABLE fs_nodes DROP COLUMN IF EXISTS path`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		// Recreates the column but NOT its contents — a true rollback also
		// requires re-running a path backfill from parent_id/name.
		return execAll(ctx, exec, []string{
			`ALTER TABLE fs_nodes ADD COLUMN IF NOT EXISTS path TEXT NOT NULL DEFAULT ''`,
			`CREATE INDEX IF NOT EXISTS fs_nodes_path_idx ON fs_nodes (tenant_id, path text_pattern_ops)`,
		})
	},
}
