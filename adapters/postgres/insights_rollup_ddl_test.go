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
		"PRIMARY KEY (tenant_id, scope_id, bucket_start, status)",
		// Runtime RLS statements (mirrors ddl.go's EnsureDynamic pattern).
		"ALTER TABLE fabriq_insights_rollup_revenue ENABLE ROW LEVEL SECURITY",
		"ALTER TABLE fabriq_insights_rollup_revenue FORCE ROW LEVEL SECURITY",
		"DROP POLICY IF EXISTS tenant_isolation ON fabriq_insights_rollup_revenue",
		"CREATE POLICY tenant_isolation ON fabriq_insights_rollup_revenue USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))",
	} {
		if !strings.Contains(all, want) {
			t.Fatalf("rollupTableDDL: want statements to contain %q, got:\n%s", want, all)
		}
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

func TestRollupTableDDL_RejectsSketchMeasures(t *testing.T) {
	for _, kind := range []string{"count_distinct", "percentile"} {
		m := revenueMetric()
		m.Measures = []registry.MetricMeasure{{Kind: kind, Field: "amount", As: "x"}}
		if _, err := rollupTableDDL(m); err == nil {
			t.Fatalf("rollupTableDDL: want error for sketch measure kind %q, got nil", kind)
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
