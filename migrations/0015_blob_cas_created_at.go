package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0015BlobCASCreatedAt = &migrate.Migration{
	Name:    "blob_cas_created_at",
	Version: "202606180015",
	Comment: "add blob_cas.created_at for the reconciler GC grace window",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE blob_cas ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE blob_cas DROP COLUMN IF EXISTS created_at`,
		})
	},
}
