//go:build integration

package postgres_test

// TestEnsureRollupTable_Idempotent proves (*postgres.Adapter).EnsureRollupTable
// against a real Postgres: it creates the per-metric rollup table with the
// expected additive columns and RLS, and running it a second time (the
// maintainer calls this at every boot, not just the first) is a no-op that
// does not error — CREATE TABLE IF NOT EXISTS + idempotent RLS statements.

import (
	"context"
	"strings"
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

	if !rollupScopeIDNullable(t, owner, table) {
		t.Errorf("rollup table %q: want scope_id column to be nullable (NULL means shared, matching fabriq_insights_events)", table)
	}

	if !rollupHasUniqueIndex(t, owner, table, table+"_uniq") {
		t.Errorf("rollup table %q: want unique index %q on (tenant_id, scope_id, bucket_start, status)", table, table+"_uniq")
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
	if !rollupPolicyIsScopeAware(t, owner, table) {
		t.Errorf("rollup table %q: want the tenant_isolation policy to be scope-aware (scope_id IS NULL OR scope_id = current_setting('app.scope_id', true)), like fabriq_insights_events/facts/rollup_state", table)
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

// rollupPolicyIsScopeAware reads the tenant_isolation policy's USING
// expression back from pg_policies and checks it carries the scope-aware
// predicate (the "scope_id IS NULL OR scope_id = current_setting(...)"
// branch), not just the plain tenant-only predicate. Postgres deparses the
// stored expression with its own whitespace/parenthesization/casts, so this
// checks for the predicate's distinguishing substrings rather than an exact
// string match.
func rollupPolicyIsScopeAware(t *testing.T, a *postgres.Adapter, table string) bool {
	t.Helper()
	var qual string
	row := a.Driver().QueryRow(context.Background(),
		`SELECT qual FROM pg_policies WHERE tablename = $1 AND policyname = 'tenant_isolation'`, table)
	if err := row.Scan(&qual); err != nil {
		t.Fatalf("read tenant_isolation policy qual for %q: %v", table, err)
	}
	return strings.Contains(qual, "scope_id") &&
		strings.Contains(qual, "IS NULL") &&
		strings.Contains(qual, "app.scope_id")
}

// rollupHasUniqueIndex reports whether table carries a unique index named
// indexName (via pg_indexes), the upsert conflict target Task 4's
// maintainer will use.
func rollupHasUniqueIndex(t *testing.T, a *postgres.Adapter, table, indexName string) bool {
	t.Helper()
	var n int
	row := a.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2`, table, indexName)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count index %q on %q: %v", indexName, table, err)
	}
	return n > 0
}

// rollupScopeIDNullable reports whether table's scope_id column is nullable
// (information_schema.columns.is_nullable = 'YES') — it must stay nullable
// so NULL can mean "shared across all scopes", matching
// fabriq_insights_events's convention. A rollup table's scope_id must never
// be a PRIMARY KEY member, which would force it NOT NULL implicitly.
func rollupScopeIDNullable(t *testing.T, a *postgres.Adapter, table string) bool {
	t.Helper()
	var nullable string
	row := a.Driver().QueryRow(context.Background(),
		`SELECT is_nullable FROM information_schema.columns WHERE table_name = $1 AND column_name = 'scope_id'`, table)
	if err := row.Scan(&nullable); err != nil {
		t.Fatalf("read scope_id is_nullable for %q: %v", table, err)
	}
	return nullable == "YES"
}
