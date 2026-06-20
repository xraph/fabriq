package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

var migration0014Blob = &migrate.Migration{
	Name:    "blob",
	Version: "202606180014",
	Comment: "blob_object aggregate + blob_cas per-tenant CAS ledger with RLS",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		stmts := append([]string{
			`CREATE TABLE IF NOT EXISTS blob_objects (
				id           TEXT PRIMARY KEY,
				tenant_id    TEXT NOT NULL,
				version      BIGINT NOT NULL,
				hash         TEXT NOT NULL,
				size         BIGINT NOT NULL,
				content_type TEXT NOT NULL DEFAULT '',
				scope_id     TEXT
			)`,
			`CREATE INDEX IF NOT EXISTS blob_objects_tenant_idx ON blob_objects (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS blob_objects_hash_idx ON blob_objects (tenant_id, hash)`,
			`CREATE TABLE IF NOT EXISTS blob_cas (
				id        TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				scope_id  TEXT,
				hash      TEXT NOT NULL,
				bucket    TEXT NOT NULL,
				key       TEXT NOT NULL,
				size      BIGINT NOT NULL,
				ref_count BIGINT NOT NULL DEFAULT 0,
				pinned    BOOLEAN NOT NULL DEFAULT FALSE
			)`,
			`CREATE UNIQUE INDEX IF NOT EXISTS blob_cas_tenant_hash_idx ON blob_cas (tenant_id, hash)`,
			`CREATE INDEX IF NOT EXISTS blob_cas_gc_idx ON blob_cas (tenant_id, ref_count, pinned) WHERE ref_count = 0 AND pinned = FALSE`,
			// blob_cas: hard tenant-only RLS (internal ledger, no scope filter)
			`ALTER TABLE blob_cas ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE blob_cas FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON blob_cas`,
			`CREATE POLICY tenant_isolation ON blob_cas
				USING ( tenant_id = current_setting('app.tenant_id', true) )
				WITH CHECK ( tenant_id = current_setting('app.tenant_id', true) )`,
			// blob_objects: scope-aware policy (matches other aggregates)
		}, ScopeAwareTenantPolicy("blob_objects")...)
		return execAll(ctx, exec, stmts)
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS blob_cas`,
			`DROP TABLE IF EXISTS blob_objects`,
		})
	},
}
