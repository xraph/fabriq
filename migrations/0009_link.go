package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// migration0009Link adds the demo `links` table — a generic reified-edge
// entity (domain.Link) projected via GraphEdgeSpec. Columns mirror the grove
// tags on domain/link.go; the registry-conformance test fails on drift.
var migration0009Link = &migrate.Migration{
	Name:    "link",
	Version: "202606120009",
	Comment: "demo reified-edge table (domain.Link) + RLS",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS links (
				id         TEXT PRIMARY KEY,
				tenant_id  TEXT NOT NULL,
				version    BIGINT NOT NULL,
				kind       TEXT NOT NULL,
				source_id  TEXT NOT NULL,
				target_id  TEXT NOT NULL,
				note       TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS links_tenant_idx ON links (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS links_source_idx ON links (tenant_id, source_id)`,
			`CREATE INDEX IF NOT EXISTS links_target_idx ON links (tenant_id, target_id)`,
			`ALTER TABLE links ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE links FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON links`,
			`CREATE POLICY tenant_isolation ON links
				USING (tenant_id = current_setting('app.tenant_id', true))
				WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS links`,
		})
	},
}
