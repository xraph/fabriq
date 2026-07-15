//go:build integration

package migrations_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestInsightsRollupMigration_UpCreatesRLSTable runs the full migration
// chain against a fresh database and asserts that the rollup watermark
// state table exists and carries row-level security.
func TestInsightsRollupMigration_UpCreatesRLSTable(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		t.Fatalf("open pg: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const table = "fabriq_insights_rollup_state"

	var n int
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table,
	).Scan(&n); err != nil {
		t.Fatalf("check %s exists: %v", table, err)
	}
	if n != 1 {
		t.Errorf("table %s missing after migrate", table)
	}

	var rls bool
	if err := db.QueryRow(ctx,
		`SELECT relrowsecurity FROM pg_class WHERE relname = $1`, table,
	).Scan(&rls); err != nil {
		t.Fatalf("check %s RLS: %v", table, err)
	}
	if !rls {
		t.Errorf("table %s must have row level security enabled", table)
	}
}

// TestInsightsRollupMigration_DownDropsTable asserts the migration is
// reversible: rolling back the chain to before 0032 removes the table.
func TestInsightsRollupMigration_DownDropsTable(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		t.Fatalf("open pg: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	rb, err := orch.Rollback(ctx)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if len(rb.Rollback) == 0 {
		t.Fatal("rollback rolled back nothing")
	}

	const table = "fabriq_insights_rollup_state"

	var n int
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table,
	).Scan(&n); err != nil {
		t.Fatalf("check %s absent: %v", table, err)
	}
	if n != 0 {
		t.Errorf("table %s still present after rolling back migration 0032", table)
	}
}
