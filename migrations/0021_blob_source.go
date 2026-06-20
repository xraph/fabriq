package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0021BlobSource = &migrate.Migration{
	Name:    "blob_source",
	Version: "202606190021",
	Comment: "blob_source external-storage connection records (encrypted auth)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := append([]string{
			`CREATE TABLE IF NOT EXISTS blob_sources (
				id           TEXT PRIMARY KEY,
				tenant_id    TEXT NOT NULL,
				scope_id     TEXT,
				version      BIGINT NOT NULL,
				project_id   TEXT NOT NULL DEFAULT '',
				name         TEXT NOT NULL,
				provider     TEXT NOT NULL DEFAULT '',
				endpoint     TEXT NOT NULL DEFAULT '',
				base_path    TEXT NOT NULL DEFAULT '',
				auth_enc     BYTEA,
				watch_config JSONB NOT NULL DEFAULT '{}',
				file_filter  JSONB NOT NULL DEFAULT '{}',
				tags         JSONB NOT NULL DEFAULT '{}',
				enabled      BOOLEAN NOT NULL DEFAULT TRUE
			)`,
			`CREATE INDEX IF NOT EXISTS blob_sources_tenant_idx ON blob_sources (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS blob_sources_project_idx ON blob_sources (tenant_id, project_id)`,
		}, ScopeAwareTenantPolicy("blob_sources")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP TABLE IF EXISTS blob_sources`})
	},
}
