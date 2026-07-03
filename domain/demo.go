package domain

// DemoDDL returns the DDL for the example domain's materialization targets
// (sites, assets, tags, tag_readings) — the tables migrations 0003–0005
// used to ship before they were evicted from the default chain (spec
// 2026-07-03 db-per-tenant, Phase 1): fabriq must not create unprefixed
// demo tables inside a database it shares with a host application.
//
// Test harnesses and the demo binaries apply this as the table owner
// (fabriqtest.ApplyDDL) BEFORE CreateAppRole, exactly like PagesDDL. The
// statements are idempotent and mirror fabriq's standard tenant-isolation
// pattern: ENABLE+FORCE RLS with USING/WITH CHECK on app.tenant_id for the
// entity tables; tag_readings stays the documented ADR 0006 exception
// (Timescale columnstore forbids RLS — isolation is structural via the
// TSQuerier plus the raw-SQL guard) and becomes a hypertable only when the
// timescaledb extension is available.
func DemoDDL() []string {
	ddl := make([]string, 0, 24)
	ddl = append(ddl,
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

		// Telemetry readings (final shape incl. the 0012 scope column).
		// Written only through the bulk event-bypass path.
		`CREATE TABLE IF NOT EXISTS tag_readings (
			time      TIMESTAMPTZ NOT NULL,
			tenant_id TEXT NOT NULL,
			tag_id    TEXT NOT NULL,
			value     DOUBLE PRECISION NOT NULL,
			quality   INT NOT NULL DEFAULT 0,
			scope_id  TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS tag_readings_idx ON tag_readings (tenant_id, tag_id, time DESC)`,
	)

	// Tenant RLS for the entity tables (the pattern 0004 used to apply).
	for _, table := range []string{"sites", "assets", "tags"} {
		ddl = append(ddl,
			`ALTER TABLE `+table+` ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE `+table+` FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON `+table,
			`CREATE POLICY tenant_isolation ON `+table+`
				USING (tenant_id = current_setting('app.tenant_id', true))
				WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
		)
	}

	// Hypertable + compression, only when timescaledb is available (the
	// 0005 behavior, expressed as a guarded DO block so plain-Postgres
	// environments apply cleanly).
	ddl = append(ddl, `DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
				CREATE EXTENSION IF NOT EXISTS timescaledb;
				PERFORM create_hypertable('tag_readings', 'time', if_not_exists => TRUE, migrate_data => TRUE);
			END IF;
		END
		$$`)

	return ddl
}
