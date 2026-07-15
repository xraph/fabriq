package postgres

import (
	"testing"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// rollupCompatMetric is the fixture MetricSpec shared by every
// TestRollupCompatible_* case: an hourly-grain rollup with two declared
// dimensions ("status", "region") — enough to exercise the Dimensions-subset
// rule (a query naming only one of the two, or a third dimension the metric
// never declared) and the Filter-column rule (a filter over a declared
// dimension vs. an ad-hoc prop).
func rollupCompatMetric() *registry.MetricSpec {
	return &registry.MetricSpec{
		Name:       "revenue",
		Source:     "order_placed",
		Dimensions: []string{"status", "region"},
		Measures: []registry.MetricMeasure{
			{Kind: "sum", Field: "amount", As: "rev"},
			{Kind: "count", As: "n"},
		},
		Rollup: &registry.RollupSpec{Bucket: time.Hour},
	}
}

func TestRollupCompatible_ExactGrainMatch(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: time.Hour}
	if !rollupCompatible(q, m) {
		t.Fatalf("TimeBucket == Rollup.Bucket (identity re-bucket): want compatible")
	}
}

func TestRollupCompatible_CoarserWholeMultiple(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: 24 * time.Hour} // 24h = 24 * 1h
	if !rollupCompatible(q, m) {
		t.Fatalf("TimeBucket == 24x Rollup.Bucket: want compatible")
	}
}

func TestRollupCompatible_DimensionsSubset(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: time.Hour, Dimensions: []string{"status"}}
	if !rollupCompatible(q, m) {
		t.Fatalf("Dimensions subset of m.Dimensions: want compatible")
	}
}

func TestRollupCompatible_FilterOverDimension(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{
		Source:     m.Name,
		TimeBucket: time.Hour,
		Filter:     query.Where{query.Eq("status", "ok")},
	}
	if !rollupCompatible(q, m) {
		t.Fatalf("Filter over a declared dimension: want compatible")
	}
}

func TestRollupCompatible_FilterOverDimensionInOrGroup(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{
		Source:     m.Name,
		TimeBucket: time.Hour,
		Filter:     query.Where{query.Or(query.Eq("status", "ok"), query.Eq("region", "us"))},
	}
	if !rollupCompatible(q, m) {
		t.Fatalf("Filter OR-group over declared dimensions: want compatible")
	}
}

func TestRollupCompatible_NilRollup(t *testing.T) {
	m := rollupCompatMetric()
	m.Rollup = nil
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: time.Hour}
	if rollupCompatible(q, m) {
		t.Fatalf("metric with no RollupSpec: want NOT compatible")
	}
}

func TestRollupCompatible_FinerThanGrain(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: 30 * time.Minute} // < 1h grain
	if rollupCompatible(q, m) {
		t.Fatalf("TimeBucket finer than Rollup.Bucket: want NOT compatible")
	}
}

func TestRollupCompatible_NotWholeMultiple(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: 90 * time.Minute} // 90m not a multiple of 1h
	if rollupCompatible(q, m) {
		t.Fatalf("TimeBucket not a whole multiple of Rollup.Bucket: want NOT compatible")
	}
}

func TestRollupCompatible_ZeroTimeBucket(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name} // TimeBucket unset (0) — no bucketing at all
	if rollupCompatible(q, m) {
		t.Fatalf("TimeBucket == 0 (not a POSITIVE multiple): want NOT compatible")
	}
}

func TestRollupCompatible_DimensionNotInRollup(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{Source: m.Name, TimeBucket: time.Hour, Dimensions: []string{"status", "channel"}}
	if rollupCompatible(q, m) {
		t.Fatalf("Dimensions includes %q, not declared on the metric: want NOT compatible", "channel")
	}
}

func TestRollupCompatible_FilterOverNonDimensionProp(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{
		Source:     m.Name,
		TimeBucket: time.Hour,
		Filter:     query.Where{query.Gt("amount", 100)}, // "amount" is a measure field, not a dimension
	}
	if rollupCompatible(q, m) {
		t.Fatalf("Filter over a non-dimension prop (measure field): want NOT compatible")
	}
}

func TestRollupCompatible_FilterOverNonDimensionInOrGroup(t *testing.T) {
	m := rollupCompatMetric()
	q := query.AnalyticsQuery{
		Source:     m.Name,
		TimeBucket: time.Hour,
		Filter:     query.Where{query.Or(query.Eq("status", "ok"), query.Eq("channel", "web"))},
	}
	if rollupCompatible(q, m) {
		t.Fatalf("Filter OR-group with one non-dimension leg: want NOT compatible")
	}
}
