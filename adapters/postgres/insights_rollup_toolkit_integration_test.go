//go:build integration

package postgres

// TestToolkitAvailable proves toolkitAvailable against a real Postgres: the
// integration harness's image (timescale/timescaledb-ha:pg16-all, see
// fabriqtest.PostgresImage) bundles timescaledb_toolkit, so the probe must
// report it available even before anything ever runs
// `CREATE EXTENSION timescaledb_toolkit`. This is an internal-package test
// (not postgres_test) because toolkitAvailable is unexported — the seam a
// caller-facing boot check (EnsureRollupTable) uses to fail loudly rather
// than let a bare driver error ("type hyperloglog does not exist") surface
// when a Rollup metric declares a sketch measure but the toolkit isn't
// installed.

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestToolkitAvailable(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	a, err := Open(ctx, dsn, registry.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	available, err := a.toolkitAvailable(ctx)
	if err != nil {
		t.Fatalf("toolkitAvailable: %v", err)
	}
	if !available {
		t.Fatal("toolkitAvailable: want true against the pg16-all image (bundles timescaledb_toolkit), got false")
	}
}

// TestEnsureRollupTable_SketchMeasures_CreatesToolkitExtension proves
// EnsureRollupTable, for a metric declaring a sketch measure, creates the
// timescaledb_toolkit extension as a side effect (not just the table) —
// exercising metricHasSketchMeasure's gate directly at the internal-package
// level, complementing the postgres_test-level column-shape assertions in
// insights_rollup_ddl_integration_test.go.
//
// The pg16-all image pre-installs timescaledb_toolkit as a CREATEd extension
// (not merely available), so this test explicitly DROPs it first to exercise
// EnsureRollupTable's own `CREATE EXTENSION IF NOT EXISTS` step rather than
// asserting a no-op against an already-installed extension.
func TestEnsureRollupTable_SketchMeasures_CreatesToolkitExtension(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	a, err := Open(ctx, dsn, registry.New())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	if _, err := a.pg.Exec(ctx, `DROP EXTENSION IF EXISTS timescaledb_toolkit`); err != nil {
		t.Fatalf("drop timescaledb_toolkit (test setup): %v", err)
	}
	var before int
	if err := a.pg.QueryRow(ctx, `SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb_toolkit'`).Scan(&before); err != nil {
		t.Fatalf("count pg_extension before: %v", err)
	}
	if before != 0 {
		t.Fatalf("want timescaledb_toolkit NOT installed after DROP EXTENSION (test setup), got count=%d", before)
	}

	m := &registry.MetricSpec{
		Name:   "unique_visitors_toolkit",
		Source: "page_viewed",
		Measures: []registry.MetricMeasure{
			{Kind: "count_distinct", Field: "visitor_id", As: "uniques"},
		},
		Rollup: &registry.RollupSpec{Bucket: time.Hour},
	}
	if err := a.EnsureRollupTable(ctx, m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	var after int
	if err := a.pg.QueryRow(ctx, `SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb_toolkit'`).Scan(&after); err != nil {
		t.Fatalf("count pg_extension after: %v", err)
	}
	if after == 0 {
		t.Fatal("want timescaledb_toolkit installed after EnsureRollupTable for a metric with a sketch measure, got still absent")
	}
}
