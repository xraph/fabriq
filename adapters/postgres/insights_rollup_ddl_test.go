package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

// revenueMetric mirrors the task-3 brief's example metric: Source
// "order_placed", one dimension ("status"), and three measures exercising
// every additive-column shape rollupTableDDL must emit — sum (single
// column), count with no Field (defaulted alias), and avg (decomposed into
// __sum/__count).
func revenueMetric() *registry.MetricSpec {
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

func TestRollupTableName(t *testing.T) {
	got, err := rollupTableName("revenue")
	if err != nil {
		t.Fatalf("rollupTableName: %v", err)
	}
	if got != "fabriq_insights_rollup_revenue" {
		t.Fatalf("want fabriq_insights_rollup_revenue, got %q", got)
	}
}

func TestRollupTableName_RejectsInvalidMetric(t *testing.T) {
	for _, bad := range []string{"bad-name", "x; DROP TABLE users", "", "has space"} {
		if _, err := rollupTableName(bad); err == nil {
			t.Fatalf("rollupTableName(%q): want error, got nil", bad)
		}
	}
}

func TestRollupTableDDL_Revenue(t *testing.T) {
	stmts, err := rollupTableDDL(revenueMetric())
	if err != nil {
		t.Fatalf("rollupTableDDL: %v", err)
	}
	if len(stmts) == 0 {
		t.Fatal("rollupTableDDL: want at least one statement")
	}
	all := strings.Join(stmts, "\n")

	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS fabriq_insights_rollup_revenue",
		"tenant_id TEXT NOT NULL",
		"scope_id TEXT",
		"bucket_start TIMESTAMPTZ NOT NULL",
		"status TEXT",
		"rev NUMERIC",
		"n NUMERIC",
		"lat__sum NUMERIC",
		"lat__count NUMERIC",
		// Uniqueness is a UNIQUE INDEX (not a PRIMARY KEY, which would force
		// scope_id NOT NULL) with NULLS NOT DISTINCT so an unscoped upsert
		// coalesces onto a single row instead of Postgres's default
		// every-NULL-is-distinct behavior.
		"CREATE UNIQUE INDEX IF NOT EXISTS fabriq_insights_rollup_revenue_uniq ON fabriq_insights_rollup_revenue (tenant_id, scope_id, bucket_start, status) NULLS NOT DISTINCT",
		// Runtime RLS statements — scope-aware (mirrors
		// migrations.ScopeAwareTenantPolicy's exact SQL text, inlined).
		"ALTER TABLE fabriq_insights_rollup_revenue ENABLE ROW LEVEL SECURITY",
		"ALTER TABLE fabriq_insights_rollup_revenue FORCE ROW LEVEL SECURITY",
		"DROP POLICY IF EXISTS tenant_isolation ON fabriq_insights_rollup_revenue",
		"CREATE POLICY tenant_isolation ON fabriq_insights_rollup_revenue",
		"tenant_id = current_setting('app.tenant_id', true)",
		"current_setting('app.scope_id', true) = ''",
		"OR scope_id IS NULL",
		"OR scope_id = current_setting('app.scope_id', true)",
		"WITH CHECK ( tenant_id = current_setting('app.tenant_id', true) )",
	} {
		if !strings.Contains(all, want) {
			t.Fatalf("rollupTableDDL: want statements to contain %q, got:\n%s", want, all)
		}
	}

	// PRIMARY KEY must be entirely absent — scope_id must stay nullable, and
	// a PRIMARY KEY would force it NOT NULL even without an explicit
	// constraint.
	if strings.Contains(all, "PRIMARY KEY") {
		t.Fatalf("rollupTableDDL: want no PRIMARY KEY (scope_id must stay nullable), got:\n%s", all)
	}

	// count's NUMERIC column must not collide with the decomposed avg
	// columns — spot check the exact "n NUMERIC" token (not just a
	// substring of something else) is present as a distinct column.
	if !strings.Contains(all, "\tn NUMERIC") && !strings.Contains(all, " n NUMERIC") {
		t.Fatalf("rollupTableDDL: want a distinct %q column, got:\n%s", "n NUMERIC", all)
	}
}

func TestRollupTableDDL_RejectsInvalidMetricName(t *testing.T) {
	m := revenueMetric()
	m.Name = "bad-name"
	if _, err := rollupTableDDL(m); err == nil {
		t.Fatal("rollupTableDDL: want error for invalid metric name, got nil")
	}
}

func TestRollupTableDDL_RejectsOverlongDerivedIndexName(t *testing.T) {
	// rollupTableName only validates the table name itself (prefix + metric,
	// <=64 chars) against ddlValid; it does not reserve headroom for the
	// "_uniq" suffix appended later. A metric name that leaves the table
	// name valid (<=64 chars) but the table name + "_uniq" over 64 chars
	// must be caught by rollupTableDDL's own ddlValid check on the derived
	// index name, not silently truncated or passed through to Postgres.
	m := revenueMetric()
	m.Name = strings.Repeat("m", 40) // table name = 23 + 40 = 63 (valid); +"_uniq" = 68 (invalid)
	if _, err := rollupTableDDL(m); err == nil {
		t.Fatal("rollupTableDDL: want error for a derived unique-index name that overflows the identifier length limit, got nil")
	}
}

func TestRollupTableDDL_RejectsInvalidDimension(t *testing.T) {
	m := revenueMetric()
	m.Dimensions = []string{"x; DROP TABLE users"}
	if _, err := rollupTableDDL(m); err == nil {
		t.Fatal("rollupTableDDL: want error for invalid dimension name, got nil")
	}
}

func TestRollupTableDDL_RejectsInvalidMeasureAlias(t *testing.T) {
	m := revenueMetric()
	m.Measures = []registry.MetricMeasure{{Kind: "sum", Field: "amount", As: "bad-alias"}}
	if _, err := rollupTableDDL(m); err == nil {
		t.Fatal("rollupTableDDL: want error for invalid measure alias, got nil")
	}
}

func TestRollupTableDDL_RejectsInvalidDefaultedMeasureAlias(t *testing.T) {
	// No explicit As, so the default alias is "<kind>_<field>" — an invalid
	// Field must still be caught (the injection guard applies to defaulted
	// aliases too, not just explicit ones).
	m := revenueMetric()
	m.Measures = []registry.MetricMeasure{{Kind: "sum", Field: "bad field"}}
	if _, err := rollupTableDDL(m); err == nil {
		t.Fatal("rollupTableDDL: want error for invalid defaulted measure alias, got nil")
	}
}

// TestRollupTableDDL_SketchColumns asserts a metric with a count_distinct
// measure and a percentile measure emits toolkit-typed columns — hyperloglog
// for count_distinct, tdigest for percentile — rather than NUMERIC. Phase
// 2b-1 rejected these Kinds outright (see the removed
// TestRollupTableDDL_RejectsSketchMeasures); phase 2b-2 stores them via
// timescaledb_toolkit.
func TestRollupTableDDL_SketchColumns(t *testing.T) {
	m := &registry.MetricSpec{
		Name:   "latency_stats",
		Source: "request_completed",
		Measures: []registry.MetricMeasure{
			{Kind: "count_distinct", Field: "visitor_id", As: "uniques"},
			{Kind: "percentile", Field: "duration_ms", As: "p50", Percentile: 0.5},
		},
		Rollup: &registry.RollupSpec{Bucket: time.Minute},
	}
	stmts, err := rollupTableDDL(m)
	if err != nil {
		t.Fatalf("rollupTableDDL: %v", err)
	}
	all := strings.Join(stmts, "\n")
	for _, want := range []string{"uniques hyperloglog", "p50 tdigest"} {
		if !strings.Contains(all, want) {
			t.Fatalf("rollupTableDDL: want %q, got:\n%s", want, all)
		}
	}
	if strings.Contains(all, "uniques NUMERIC") || strings.Contains(all, "p50 NUMERIC") {
		t.Fatalf("rollupTableDDL: sketch columns must NOT be NUMERIC, got:\n%s", all)
	}
}

// TestRollupTableDDL_SketchMeasureDefaultAlias asserts the default alias
// ("<kind>_<field>") applies to sketch measures too, same as additive ones.
func TestRollupTableDDL_SketchMeasureDefaultAlias(t *testing.T) {
	m := &registry.MetricSpec{
		Name:     "unique_visitors",
		Source:   "page_viewed",
		Measures: []registry.MetricMeasure{{Kind: "count_distinct", Field: "visitor_id"}},
		Rollup:   &registry.RollupSpec{Bucket: time.Hour},
	}
	stmts, err := rollupTableDDL(m)
	if err != nil {
		t.Fatalf("rollupTableDDL: %v", err)
	}
	all := strings.Join(stmts, "\n")
	if !strings.Contains(all, "count_distinct_visitor_id hyperloglog") {
		t.Fatalf("rollupTableDDL: want defaulted alias %q, got:\n%s", "count_distinct_visitor_id hyperloglog", all)
	}
}

func TestRollupTableDDL_RejectsInvalidSketchMeasureAlias(t *testing.T) {
	for _, kind := range []string{"count_distinct", "percentile"} {
		m := revenueMetric()
		m.Measures = []registry.MetricMeasure{{Kind: kind, Field: "amount", As: "bad-alias"}}
		if _, err := rollupTableDDL(m); err == nil {
			t.Fatalf("rollupTableDDL: want error for invalid sketch measure alias (kind %q), got nil", kind)
		}
	}
}

func TestRollupTableDDL_DefaultCountAlias(t *testing.T) {
	m := &registry.MetricSpec{
		Name:     "signups",
		Source:   "user_signed_up",
		Measures: []registry.MetricMeasure{{Kind: "count"}},
		Rollup:   &registry.RollupSpec{Bucket: time.Hour},
	}
	stmts, err := rollupTableDDL(m)
	if err != nil {
		t.Fatalf("rollupTableDDL: %v", err)
	}
	all := strings.Join(stmts, "\n")
	if !strings.Contains(all, "count NUMERIC") {
		t.Fatalf("rollupTableDDL: want a defaulted %q column for a count measure with no As, got:\n%s", "count NUMERIC", all)
	}
}

func TestRollupTableDDL_MinMaxColumns(t *testing.T) {
	m := &registry.MetricSpec{
		Name:     "latency_stats",
		Source:   "request_completed",
		Measures: []registry.MetricMeasure{{Kind: "min", Field: "duration_ms", As: "min_dur"}, {Kind: "max", Field: "duration_ms", As: "max_dur"}},
		Rollup:   &registry.RollupSpec{Bucket: time.Minute},
	}
	stmts, err := rollupTableDDL(m)
	if err != nil {
		t.Fatalf("rollupTableDDL: %v", err)
	}
	all := strings.Join(stmts, "\n")
	for _, want := range []string{"min_dur NUMERIC", "max_dur NUMERIC"} {
		if !strings.Contains(all, want) {
			t.Fatalf("rollupTableDDL: want %q, got:\n%s", want, all)
		}
	}
}
