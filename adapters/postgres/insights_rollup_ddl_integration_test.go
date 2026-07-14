//go:build integration

package postgres_test

// TestEnsureRollupTable_Idempotent proves (*postgres.Adapter).EnsureRollupTable
// against a real Postgres: it creates the per-metric rollup table with the
// expected additive columns and RLS, and running it a second time (the
// maintainer calls this at every boot, not just the first) is a no-op that
// does not error — CREATE TABLE IF NOT EXISTS + idempotent RLS statements.

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
)

// revenueRollupMetric mirrors the task-3 brief's example metric used by the
// pure rollupTableDDL unit tests in insights_rollup_ddl_test.go — kept in
// sync deliberately so the integration test proves the SAME shape actually
// lands in a real database.
func revenueRollupMetric() *registry.MetricSpec {
	return &registry.MetricSpec{
		Name:       "revenue",
		Source:     "order_placed",
		Dimensions: []string{"status"},
		Measures: []registry.MetricMeasure{
			{Kind: "sum", Field: "amount", As: "rev"},
			{Kind: "count", As: "n"},
			{Kind: "avg", Field: "latency", As: "lat"},
		},
		Rollup: &registry.RollupSpec{Bucket: time.Hour},
	}
}

func TestEnsureRollupTable_Idempotent(t *testing.T) {
	ctx := context.Background()
	_, owner := newInsightsHarness(t, registry.New())

	m := revenueRollupMetric()
	const table = "fabriq_insights_rollup_revenue"

	if err := owner.EnsureRollupTable(ctx, m); err != nil {
		t.Fatalf("EnsureRollupTable (1st call): %v", err)
	}
	if err := owner.EnsureRollupTable(ctx, m); err != nil {
		t.Fatalf("EnsureRollupTable (2nd call, must be idempotent): %v", err)
	}

	cols := rollupColumnSet(t, owner, table)
	for _, want := range []string{
		"tenant_id", "scope_id", "bucket_start", "status",
		"rev", "n", "lat__sum", "lat__count",
	} {
		if !cols[want] {
			t.Errorf("rollup table %q missing expected column %q; got columns: %v", table, want, cols)
		}
	}

	enabled, forced := rollupRLSFlags(t, owner, table)
	if !enabled {
		t.Errorf("rollup table %q: want ROW LEVEL SECURITY enabled", table)
	}
	if !forced {
		t.Errorf("rollup table %q: want ROW LEVEL SECURITY forced", table)
	}
	if !rollupHasTenantIsolationPolicy(t, owner, table) {
		t.Errorf("rollup table %q: want a tenant_isolation policy", table)
	}
}

// rollupColumnSet reads back the physical columns of table via
// information_schema, keyed by column_name for O(1) membership checks.
func rollupColumnSet(t *testing.T, a *postgres.Adapter, table string) map[string]bool {
	t.Helper()
	rows, err := a.Driver().Query(context.Background(),
		`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, table)
	if err != nil {
		t.Fatalf("query information_schema.columns for %q: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan column_name: %v", err)
		}
		out[c] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate information_schema.columns rows: %v", err)
	}
	return out
}

// rollupRLSFlags reads pg_class.relrowsecurity/relforcerowsecurity for table.
func rollupRLSFlags(t *testing.T, a *postgres.Adapter, table string) (enabled, forced bool) {
	t.Helper()
	row := a.Driver().QueryRow(context.Background(),
		`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1`, table)
	if err := row.Scan(&enabled, &forced); err != nil {
		t.Fatalf("read pg_class RLS flags for %q: %v", table, err)
	}
	return enabled, forced
}

// rollupHasTenantIsolationPolicy reports whether table carries a policy named
// "tenant_isolation" (via pg_policies, the same view namespace_integration_test.go
// uses to verify RLS-following-rename).
func rollupHasTenantIsolationPolicy(t *testing.T, a *postgres.Adapter, table string) bool {
	t.Helper()
	var n int
	row := a.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM pg_policies WHERE tablename = $1 AND policyname = 'tenant_isolation'`, table)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count tenant_isolation policies for %q: %v", table, err)
	}
	return n > 0
}
