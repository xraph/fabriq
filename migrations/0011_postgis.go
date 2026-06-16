package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_geometries stores one geometry per (tenant, entity, id) with a GiST
// index for radius/nearest queries. Guarded on the PostGIS extension — skips
// cleanly on a plain Postgres (mirrors 0006_pgvector). SRID is stored alongside
// the geometry so callers can mix geographic (4326) and local/planar (0) frames.
var migration0011PostGIS = &migrate.Migration{
	Name:    "postgis",
	Version: "202606160011",
	Comment: "geometries table + GiST index (PostGIS)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		available, err := extensionAvailable(ctx, exec, "postgis")
		if err != nil {
			return err
		}
		if !available {
			return nil
		}
		return execAll(ctx, exec, []string{
			`CREATE EXTENSION IF NOT EXISTS postgis`,
			`CREATE TABLE IF NOT EXISTS fabriq_geometries (
				tenant_id TEXT NOT NULL,
				entity    TEXT NOT NULL,
				id        TEXT NOT NULL,
				geom      geometry NOT NULL,
				srid      INT NOT NULL,
				meta      JSONB NOT NULL DEFAULT '{}'::jsonb,
				PRIMARY KEY (tenant_id, entity, id)
			)`,
			`ALTER TABLE fabriq_geometries ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE fabriq_geometries FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON fabriq_geometries`,
			`CREATE POLICY tenant_isolation ON fabriq_geometries
				USING (tenant_id = current_setting('app.tenant_id', true))
				WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
			`CREATE INDEX IF NOT EXISTS fabriq_geometries_gist
				ON fabriq_geometries USING gist (geom)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_geometries`,
		})
	},
}
