// Package insights holds the behavioral contract for the per-tenant,
// customer-facing analytics port (query.AnalyticsQuerier). It is deliberately
// distinct from core/analytics, which is the operator-facing cross-tenant
// sink conformance suite for a different port (analytics.Sink).
package insights

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// RunConformance is the single behavioral contract every query.AnalyticsQuerier
// must satisfy. fabriqtest runs it against FakeAnalytics; adapters/postgres runs
// it against real Postgres. Drift is a failing test.
//
// It exercises Track + Query only. QueryRaw has no portable in-memory
// contract (raw SQL is dialect-specific): the adapter's own test suite
// exercises QueryRaw separately.
func RunConformance(t *testing.T, newQ func() query.AnalyticsQuerier) {
	ctx1 := mustTenant(t, "t1")
	ctx2 := mustTenant(t, "t2")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("CountBySingleDimension", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"status": "paid", "amount": 10}},
			{Name: "order", At: base, Props: map[string]any{"status": "paid", "amount": 5}},
			{Name: "order", At: base, Props: map[string]any{"status": "void", "amount": 0}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}, {Kind: query.MeasureSum, Field: "amount", As: "total"}},
			Dimensions: []string{"status"},
		}, &rows))
		paid := findRow(rows, "status", "paid")
		if paid == nil || asInt(paid["n"]) != 2 || asInt(paid["total"]) != 15 {
			t.Fatalf("paid group wrong: %+v", paid)
		}
		void := findRow(rows, "status", "void")
		if void == nil || asInt(void["n"]) != 1 || asInt(void["total"]) != 0 {
			t.Fatalf("void group wrong: %+v", void)
		}
		if len(rows) != 2 {
			t.Fatalf("want 2 groups, got %d: %+v", len(rows), rows)
		}
	})

	t.Run("TimeBucketGroups", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "hit", At: base, Props: map[string]any{}},
			{Name: "hit", At: base.Add(90 * time.Minute), Props: map[string]any{}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source: "hit", TimeBucket: time.Hour,
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows))
		if len(rows) != 2 {
			t.Fatalf("want 2 hourly buckets, got %d: %+v", len(rows), rows)
		}
		for _, r := range rows {
			if asInt(r["n"]) != 1 {
				t.Fatalf("want 1 event per hourly bucket: %+v", r)
			}
		}
	})

	t.Run("TimeWindowFrom_To", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "hit", At: base, Props: map[string]any{}},
			{Name: "hit", At: base.Add(time.Hour), Props: map[string]any{}},
			{Name: "hit", At: base.Add(2 * time.Hour), Props: map[string]any{}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "hit",
			From:     base.Add(30 * time.Minute),
			To:       base.Add(2 * time.Hour), // exclusive upper bound
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows))
		if len(rows) != 1 || asInt(rows[0]["n"]) != 1 {
			t.Fatalf("time window wrong: %+v", rows)
		}
	})

	t.Run("FilterNarrows", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"status": "paid"}},
			{Name: "order", At: base, Props: map[string]any{"status": "void"}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Eq("status", "paid")},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows))
		if len(rows) != 1 || asInt(rows[0]["n"]) != 1 {
			t.Fatalf("filter wrong: %+v", rows)
		}
	})

	t.Run("FilterIn", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"status": "paid"}},
			{Name: "order", At: base, Props: map[string]any{"status": "void"}},
			{Name: "order", At: base, Props: map[string]any{"status": "refunded"}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.In("status", []any{"paid", "refunded"})},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows))
		if len(rows) != 1 || asInt(rows[0]["n"]) != 2 {
			t.Fatalf("in-filter wrong: %+v", rows)
		}
	})

	t.Run("MinMaxAvg", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"amount": 10}},
			{Name: "order", At: base, Props: map[string]any{"amount": 20}},
			{Name: "order", At: base, Props: map[string]any{"amount": 30}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source: "order",
			Measures: []query.Measure{
				{Kind: query.MeasureMin, Field: "amount", As: "lo"},
				{Kind: query.MeasureMax, Field: "amount", As: "hi"},
				{Kind: query.MeasureAvg, Field: "amount", As: "avg"},
			},
		}, &rows))
		if len(rows) != 1 {
			t.Fatalf("want 1 grand-total row, got %d: %+v", len(rows), rows)
		}
		r := rows[0]
		if asInt(r["lo"]) != 10 || asInt(r["hi"]) != 30 {
			t.Fatalf("min/max wrong: %+v", r)
		}
		if avg, ok := toFloatT(r["avg"]); !ok || avg != 20 {
			t.Fatalf("avg wrong: %+v", r)
		}
	})

	t.Run("CountDistinct", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "visit", At: base, Props: map[string]any{"user": "a"}},
			{Name: "visit", At: base, Props: map[string]any{"user": "a"}},
			{Name: "visit", At: base, Props: map[string]any{"user": "b"}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "visit",
			Measures: []query.Measure{{Kind: query.MeasureCountDistinct, Field: "user", As: "u"}},
		}, &rows))
		if len(rows) != 1 || asInt(rows[0]["u"]) != 2 {
			t.Fatalf("count_distinct wrong: %+v", rows)
		}
	})

	t.Run("DedupKeyIgnoresReplays", func(t *testing.T) {
		q := newQ()
		ev := query.AnalyticsEvent{Name: "order", At: base, Props: map[string]any{}, DedupKey: "k1"}
		must(t, q.Track(ctx1, []query.AnalyticsEvent{ev}))
		must(t, q.Track(ctx1, []query.AnalyticsEvent{ev})) // replay
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{Source: "order", Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}}}, &rows))
		if len(rows) != 1 || asInt(rows[0]["n"]) != 1 {
			t.Fatalf("dedup failed: %+v", rows)
		}
	})

	t.Run("LimitBoundsRows", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"status": "a"}},
			{Name: "order", At: base, Props: map[string]any{"status": "b"}},
			{Name: "order", At: base, Props: map[string]any{"status": "c"}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
			Limit:      2,
		}, &rows))
		if len(rows) != 2 {
			t.Fatalf("limit not honored: %+v", rows)
		}
		// Deterministic default ordering (by dimension) means the same two
		// groups come back on every run.
		if rows[0]["status"] != "a" || rows[1]["status"] != "b" {
			t.Fatalf("default ordering wrong: %+v", rows)
		}
	})

	t.Run("TenantIsolation", func(t *testing.T) {
		q := newQ()
		must(t, q.Track(ctx1, []query.AnalyticsEvent{{Name: "order", At: base, Props: map[string]any{}}}))
		var rows []map[string]any
		must(t, q.Query(ctx2, query.AnalyticsQuery{Source: "order", Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}}}, &rows))
		// t2 tracked nothing: either zero rows or a single grand-total of 0.
		if len(rows) == 1 && asInt(rows[0]["n"]) != 0 {
			t.Fatalf("tenant isolation breached: %+v", rows)
		}
		if len(rows) > 1 {
			t.Fatalf("tenant isolation breached: %+v", rows)
		}
	})

	t.Run("NoTenantErrors", func(t *testing.T) {
		q := newQ()
		if err := q.Track(context.Background(), []query.AnalyticsEvent{{Name: "x"}}); err == nil {
			t.Fatal("want error tracking without a tenant on ctx")
		}
		var rows []map[string]any
		if err := q.Query(context.Background(), query.AnalyticsQuery{Source: "x"}, &rows); err == nil {
			t.Fatal("want error querying without a tenant on ctx")
		}
	})
}

func mustTenant(t *testing.T, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// findRow returns the first row whose column equals value, or nil.
func findRow(rows []map[string]any, column string, value any) map[string]any {
	for _, r := range rows {
		if r[column] == value {
			return r
		}
	}
	return nil
}

// toFloatT coerces common numeric representations (as they appear after a
// JSON round-trip: float64, json.Number, or Go-native ints/floats before one)
// to float64.
func toFloatT(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// toInt64 coerces common numeric representations to int64, rounding floats.
func toInt64(v any) int64 {
	f, _ := toFloatT(v)
	return int64(f)
}

// asInt handles int/int64/float64/json.Number result values (Query output
// rows carry Go-native numbers directly from the fake, and adapter-decoded
// numbers after a driver round-trip).
func asInt(v any) int64 { return toInt64(v) }
