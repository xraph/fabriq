package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Domain tables for the example domain pack. Columns mirror domain/*.go grove tags
// exactly — the registry-conformance test fails the build on drift in
// either direction. No cross-table foreign keys: referential shape is
// projected into the graph, and rebuilds replay from these tables.
var migration0003SiteAssetTag = &migrate.Migration{
	Name:    "site_asset_tag",
	Version: "202606120003",
	Comment: "Example domain tables + telemetry readings table",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS sites (
				id        TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				version   BIGINT NOT NULL,
				name      TEXT NOT NULL,
				code      TEXT NOT NULL DEFAULT '',
				region    TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS sites_tenant_idx ON sites (tenant_id)`,

			`CREATE TABLE IF NOT EXISTS assets (
				id        TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				version   BIGINT NOT NULL,
				name      TEXT NOT NULL,
				kind      TEXT NOT NULL DEFAULT '',
				serial    TEXT NOT NULL DEFAULT '',
				site_id   TEXT NOT NULL DEFAULT '',
				parent_id TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS assets_tenant_idx ON assets (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS assets_site_idx ON assets (tenant_id, site_id)`,
			`CREATE INDEX IF NOT EXISTS assets_parent_idx ON assets (tenant_id, parent_id)`,

			`CREATE TABLE IF NOT EXISTS tags (
				id        TEXT PRIMARY KEY,
				tenant_id TEXT NOT NULL,
				version   BIGINT NOT NULL,
				name      TEXT NOT NULL,
				unit      TEXT NOT NULL DEFAULT '',
				datatype  TEXT NOT NULL DEFAULT '',
				asset_id  TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS tags_tenant_idx ON tags (tenant_id)`,
			`CREATE INDEX IF NOT EXISTS tags_asset_idx ON tags (tenant_id, asset_id)`,

			// Telemetry readings: becomes a hypertable in 0005. Written only
			// through the bulk event-bypass path.
			`CREATE TABLE IF NOT EXISTS tag_readings (
				time      TIMESTAMPTZ NOT NULL,
				tenant_id TEXT NOT NULL,
				tag_id    TEXT NOT NULL,
				value     DOUBLE PRECISION NOT NULL,
				quality   INT NOT NULL DEFAULT 0
			)`,
			`CREATE INDEX IF NOT EXISTS tag_readings_idx ON tag_readings (tenant_id, tag_id, time DESC)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS tag_readings`,
			`DROP TABLE IF EXISTS tags`,
			`DROP TABLE IF EXISTS assets`,
			`DROP TABLE IF EXISTS sites`,
		})
	},
}
