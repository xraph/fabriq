//go:build integration

package migrations_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestInsightsMigration_UpCreatesRLSTables runs the full migration chain
// against a fresh database and asserts that the two in-tenant customer
// analytics tables (distinct from the operator-facing fabriq_analytics_*
// sink tables) exist and carry row-level security.
func TestInsightsMigration_UpCreatesRLSTables(t *testing.T) {
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

	for _, table := range []string{"fabriq_insights_events", "fabriq_insights_facts"} {
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
}

// TestInsightsMigration_DownDropsTables asserts the migration is reversible:
// rolling back the chain to before 0031 removes both tables.
//
// orch.Rollback rolls back exactly ONE migration per call (grove/migrate's
// Orchestrator.Rollback: "rolls back the last batch of applied migrations,
// one per group, most recently applied first" — for this single-group chain
// that means the single last-applied migration). This test only cares about
// 0031's Down (which drops both fabriq_insights_events and
// fabriq_insights_facts in one migration — see 0031_insights.go), so instead
// of assuming 0031 is HEAD (it stopped being HEAD the moment phase-2b's Task
// 2 added migration 0032 on top), it rolls back repeatedly — bounded so a
// genuine bug (Down never dropping the table) still fails fast instead of
// looping — until fabriq_insights_events is gone, then asserts both tables
// are absent. This keeps the test correct no matter how many migrations
// future work stacks on top of 0031.
func TestInsightsMigration_DownDropsTables(t *testing.T) {
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

	const maxRollbacks = 20 // generous bound: far more than any foreseeable migration count above 0031
	rolledBackAny := false
	for i := 0; i < maxRollbacks; i++ {
		var n int
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, "fabriq_insights_events",
		).Scan(&n); err != nil {
			t.Fatalf("check fabriq_insights_events absent: %v", err)
		}
		if n == 0 {
			break
		}

		rb, err := orch.Rollback(ctx)
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if len(rb.Rollback) == 0 {
			t.Fatalf("rollback rolled back nothing (ran out of migrations before fabriq_insights_events was dropped)")
		}
		rolledBackAny = true
	}
	if !rolledBackAny {
		t.Fatal("rollback rolled back nothing")
	}

	for _, table := range []string{"fabriq_insights_events", "fabriq_insights_facts"} {
		var n int
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table,
		).Scan(&n); err != nil {
			t.Fatalf("check %s absent: %v", table, err)
		}
		if n != 0 {
			t.Errorf("table %s still present after rolling back migration 0031", table)
		}
	}
}
