package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0017FsNodeMount = &migrate.Migration{
	Name:    "fs_node_mount",
	Version: "202606190017",
	Comment: "fs_nodes.mount_config for node_type=mount",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fs_nodes ADD COLUMN IF NOT EXISTS mount_config JSONB NOT NULL DEFAULT '{}'`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`ALTER TABLE fs_nodes DROP COLUMN IF EXISTS mount_config`})
	},
}
