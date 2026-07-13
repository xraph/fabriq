package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
)

// evtDesc builds the event Descriptor insights.ResolveSource would produce
// for source when no registry entity/metric claims it — the shape every
// buildInsightsSQL unit test in this file exercises (they predate
// insights.ResolveSource and test the builder in isolation from the
// resolver).
func evtDesc(source string) insights.Descriptor {
	return insights.Descriptor{
		Kind:       insights.SourceEvent,
		Table:      "fabriq_insights_events",
		JSONColumn: "props",
		KeyColumn:  "name",
		KeyValue:   source,
	}
}

func TestBuildInsightsSQL_CountByDimension(t *testing.T) {
	sql, args, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:     "order",
		Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}, {Kind: query.MeasureSum, Field: "amount", As: "total"}},
		Dimensions: []string{"status"},
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	// dimension read from props ->> 'status'; measures COUNT(*) and SUM over props->>'amount'
	for _, want := range []string{
		"FROM fabriq_insights_events",
		"tenant_id = $1",
		"name = $2",
		"props ->> 'status'",
		`COUNT(*) AS "n"`,
		`SUM((props ->> 'amount')::numeric) AS "total"`,
		"GROUP BY",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("sql missing %q:\n%s", want, sql)
		}
	}
	if args[0] != "t1" || args[1] != "order" {
		t.Fatalf("args wrong: %v", args)
	}
}

func TestBuildInsightsSQL_TimeBucket(t *testing.T) {
	sql, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source: "hit", TimeBucket: time.Hour,
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
	}, "t1", evtDesc("hit"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "time_bucket(") || !strings.Contains(sql, `AS bucket`) {
		t.Fatalf("expected time_bucket in:\n%s", sql)
	}
}

func TestBuildInsightsSQL_RejectsNoMeasures(t *testing.T) {
	if _, _, err := buildInsightsSQL(query.AnalyticsQuery{Source: "x"}, "t1", evtDesc("x")); err == nil {
		t.Fatal("want error with no measures")
	}
}

func TestBuildInsightsSQL_RejectsInjectionInDimension(t *testing.T) {
	_, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source: "x", Measures: []query.Measure{{Kind: query.MeasureCount}},
		Dimensions: []string{"status'; DROP TABLE users;--"},
	}, "t1", evtDesc("x"))
	if err == nil {
		t.Fatal("want rejection of non-identifier dimension")
	}
}

func TestBuildInsightsSQL_RejectsHaving(t *testing.T) {
	_, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "x",
		Measures: []query.Measure{{Kind: query.MeasureCount}},
		Having:   query.Where{query.Gt("count", 10)},
	}, "t1", evtDesc("x"))
	if err == nil {
		t.Fatal("want explicit rejection of Having (phase-1 gap)")
	}
}

func TestBuildInsightsSQL_FilterBindsValueRewritesColumn(t *testing.T) {
	sql, args, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "order",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Filter:   query.Where{query.Eq("status", "paid")},
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "props ->> 'status' = $3") {
		t.Fatalf("expected filter column rewritten to prop accessor with bound value:\n%s", sql)
	}
	if args[2] != "paid" {
		t.Fatalf("expected filter value bound as $3, got args=%v", args)
	}
}

func TestBuildInsightsSQL_NumericRangeFilterCastsToNumeric(t *testing.T) {
	sql, args, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "order",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Filter:   query.Where{query.Gt("amount", 100)},
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	// A numeric-valued Gt must cast the JSONB accessor to numeric before
	// comparing, or "50" > "100" wins lexicographically and silently returns
	// wrong rows (see measureExpr, which casts the same field for measures).
	if !strings.Contains(sql, "(props ->> 'amount')::numeric > $3") {
		t.Fatalf("expected numeric-cast comparison for numeric Gt bound:\n%s", sql)
	}
	if args[2] != 100 {
		t.Fatalf("expected filter value bound as $3, got args=%v", args)
	}
}

func TestBuildInsightsSQL_StringRangeFilterStaysText(t *testing.T) {
	sql, args, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "order",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Filter:   query.Where{query.Gt("col", "m")},
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	// A string-valued Gt must stay a plain text comparison, with no numeric
	// cast, so range filters over genuinely string-typed fields still work.
	if !strings.Contains(sql, "props ->> 'col' > $3") {
		t.Fatalf("expected plain text comparison for string Gt bound:\n%s", sql)
	}
	if strings.Contains(sql, "(props ->> 'col')::numeric") {
		t.Fatalf("did not expect numeric cast for string-valued Gt:\n%s", sql)
	}
	if args[2] != "m" {
		t.Fatalf("expected filter value bound as $3, got args=%v", args)
	}
}

func TestBuildInsightsSQL_RejectsInjectionInFilterColumn(t *testing.T) {
	_, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "x",
		Measures: []query.Measure{{Kind: query.MeasureCount}},
		Filter:   query.Where{query.Eq("status'; DROP TABLE users;--", "paid")},
	}, "t1", evtDesc("x"))
	if err == nil {
		t.Fatal("want rejection of non-identifier filter column")
	}
}

func TestBuildInsightsSQL_RejectsInjectionInMeasureAlias(t *testing.T) {
	_, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "x",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n; DROP TABLE users;--"}},
	}, "t1", evtDesc("x"))
	if err == nil {
		t.Fatal("want rejection of non-identifier measure alias")
	}
}

func TestBuildInsightsSQL_OrderByValidatesAgainstDeclaredColumns(t *testing.T) {
	// Valid: orders by a declared dimension and a measure alias.
	sql, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:     "order",
		Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Dimensions: []string{"status"},
		OrderBy:    "status ASC, n DESC",
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `ORDER BY "status" ASC, "n" DESC`) {
		t.Fatalf("expected validated order by clause:\n%s", sql)
	}

	// Invalid: references an undeclared column.
	_, _, err = buildInsightsSQL(query.AnalyticsQuery{
		Source:     "order",
		Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Dimensions: []string{"status"},
		OrderBy:    "not_a_column",
	}, "t1", evtDesc("order"))
	if err == nil {
		t.Fatal("want rejection of order by referencing an undeclared column")
	}
}

func TestBuildInsightsSQL_DefaultOrderByGroupsWhenNoOrderBySpecified(t *testing.T) {
	sql, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:     "order",
		Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Dimensions: []string{"status"},
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "ORDER BY") {
		t.Fatalf("expected default order by groups:\n%s", sql)
	}
}

func TestBuildInsightsSQL_FromToWindow(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	sql, args, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "order",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		From:     from,
		To:       to,
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "at >= $3") || !strings.Contains(sql, "at < $4") {
		t.Fatalf("expected bound from/to window:\n%s", sql)
	}
	if args[2] != from || args[3] != to {
		t.Fatalf("expected from/to bound as args, got %v", args)
	}
}

func TestBuildInsightsSQL_LimitOffset(t *testing.T) {
	sql, _, err := buildInsightsSQL(query.AnalyticsQuery{
		Source:   "order",
		Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Limit:    10,
		Offset:   5,
	}, "t1", evtDesc("order"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "LIMIT 10") || !strings.Contains(sql, "OFFSET 5") {
		t.Fatalf("expected limit/offset:\n%s", sql)
	}
}
