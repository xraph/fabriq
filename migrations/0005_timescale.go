package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Converts tag_readings into a hypertable with compression. Skips quietly
// when the timescaledb extension is unavailable (plain-Postgres dev
// environments still migrate; the TSQuerier then runs on a plain table).
var migration0005Timescale = &migrate.Migration{
	Name:    "timescale",
	Version: "202606120005",
	Comment: "tag_readings hypertable + compression policy",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		available, err := extensionAvailable(ctx, exec, "timescaledb")
		if err != nil {
			return err
		}
		if !available {
			return nil
		}
		return execAll(ctx, exec, []string{
			`CREATE EXTENSION IF NOT EXISTS timescaledb`,
			`SELECT create_hypertable('tag_readings', 'time', if_not_exists => TRUE, migrate_data => TRUE)`,
			`ALTER TABLE tag_readings SET (
				timescaledb.compress,
				timescaledb.compress_segmentby = 'tenant_id, tag_id',
				timescaledb.compress_orderby = 'time DESC'
			)`,
			`SELECT add_compression_policy('tag_readings', INTERVAL '7 days', if_not_exists => TRUE)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		available, err := extensionAvailable(ctx, exec, "timescaledb")
		if err != nil {
			return err
		}
		if !available {
			return nil
		}
		// Recreate the plain table; hypertable un-conversion is not a thing.
		return execAll(ctx, exec, []string{
			`SELECT remove_compression_policy('tag_readings', if_exists => TRUE)`,
		})
	},
}

func extensionAvailable(ctx context.Context, exec migrate.Executor, name string) (bool, error) {
	rows, err := exec.Query(ctx, `SELECT count(*) FROM pg_available_extensions WHERE name = $1`, name)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
	}
	return n > 0, rows.Err()
}
