package registry_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

// TestValidate_RejectsRollupBucketZero asserts a Rollup with a non-positive
// Bucket is rejected, naming both the metric and the offending field.
func TestValidate_RejectsRollupBucketZero(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "a", Schema: minimalDynSchema("as"),
		Metrics: []registry.MetricSpec{{
			Name:     "signups",
			Source:   "user_signed_up", // event source, not an entity
			Measures: []registry.MetricMeasure{{Kind: "count"}},
			Rollup:   &registry.RollupSpec{Bucket: 0},
		}},
	})
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "Bucket") || !strings.Contains(got.Error(), "signups") {
		t.Fatalf("want error naming metric and Bucket, got %v", got)
	}
}

// TestValidate_RejectsRollupBucketNotDayDivisible asserts a Rollup.Bucket
// that does not evenly divide 24h is rejected. This is the C1-fix
// precondition: the stitched-query boundary math (floorToGrain/ceilToGrain
// via Go's time.Truncate, epoch-origin) only matches Postgres
// time_bucket(grain, at)'s day-aligned grid when the grain divides a day
// evenly — a grain like 5h would silently reintroduce the boundary-drop/
// double-count bug the C1 fix closed.
func TestValidate_RejectsRollupBucketNotDayDivisible(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "a", Schema: minimalDynSchema("as"),
		Metrics: []registry.MetricSpec{{
			Name:     "signups",
			Source:   "user_signed_up",
			Measures: []registry.MetricMeasure{{Kind: "count"}},
			Rollup:   &registry.RollupSpec{Bucket: 5 * time.Hour},
		}},
	})
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "evenly divide 24h") || !strings.Contains(got.Error(), "signups") {
		t.Fatalf("want error naming metric and the day-divide constraint, got %v", got)
	}
}

// TestValidate_RejectsRollupBucketOver24h asserts a Rollup.Bucket greater
// than 24h is rejected outright (weekly/monthly aggregation is still
// achievable by querying a daily-grain rollup with a weekly TimeBucket,
// since a week is a whole multiple of a day).
func TestValidate_RejectsRollupBucketOver24h(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "a", Schema: minimalDynSchema("as"),
		Metrics: []registry.MetricSpec{{
			Name:     "signups",
			Source:   "user_signed_up",
			Measures: []registry.MetricMeasure{{Kind: "count"}},
			Rollup:   &registry.RollupSpec{Bucket: 48 * time.Hour},
		}},
	})
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "evenly divide 24h") {
		t.Fatalf("want error naming the day-divide constraint, got %v", got)
	}
}

// TestValidate_AllowsDayDivisibleRollupBuckets asserts every commonly-used
// day-dividing grain (30m, 1h, 24h) is accepted, distinct entities per grain
// so the "duplicate metric name" rule doesn't interfere.
func TestValidate_AllowsDayDivisibleRollupBuckets(t *testing.T) {
	grains := []time.Duration{30 * time.Minute, time.Hour, 24 * time.Hour}
	for i, g := range grains {
		g := g
		entityName := fmt.Sprintf("grain%d", i)
		table := fmt.Sprintf("grain%ds", i)
		r := registry.New()
		err := r.Register(registry.EntitySpec{
			Name: entityName, Schema: minimalDynSchema(table),
			Metrics: []registry.MetricSpec{{
				Name:     fmt.Sprintf("metric_%d", i),
				Source:   "some_event",
				Measures: []registry.MetricMeasure{{Kind: "count"}},
				Rollup:   &registry.RollupSpec{Bucket: g},
			}},
		})
		if got := firstErr(r, err); got != nil {
			t.Fatalf("grain %v: want accepted, got error: %v", g, got)
		}
	}
}

// TestValidate_RejectsRollupOnEntitySource asserts a Rollup metric whose
// Source names a registered ENTITY (not an event) is rejected — rollups are
// event-sourced only in 2b-1. The metric is declared ON the "order" entity
// itself (self-sourced), mirroring mustRegisterOrder's shape, since a
// MetricSpec's measure fields validate against the columns of the entity that
// DECLARES the metric, not the Source.
func TestValidate_RejectsRollupOnEntitySource(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name:  "order",
		Model: orderModel(),
		Insights: &registry.InsightsSpec{
			Measures:   []string{"amount"},
			Dimensions: []string{"status"},
		},
		Metrics: []registry.MetricSpec{{
			Name:     "order_rollup",
			Source:   "order", // registered entity, not an event
			Measures: []registry.MetricMeasure{{Kind: "sum", Field: "amount"}},
			Rollup:   &registry.RollupSpec{Bucket: time.Hour},
		}},
	})
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "event-sourced") {
		t.Fatalf("want event-sourced-only error, got %v", got)
	}
}

// sketchMeasureSchema is a DynamicSchema with the columns the sketch-measure
// tests need, so validateAndBind's "measure field is a column" check (which
// validates against the entity declaring the Metric, not its Source) passes
// and the failure under test is the Rollup/sketch-measure rule, not a column
// lookup.
func sketchMeasureSchema(table, col string) *registry.DynamicSchema {
	return &registry.DynamicSchema{
		Table:   table,
		Columns: []registry.DynamicColumn{{Name: col, Type: registry.ColFloat}},
	}
}

// TestValidate_AllowsSketchMeasureInRollup asserts a Rollup metric carrying a
// non-additive ("sketch") measure — count_distinct or percentile — now
// VALIDATES successfully. Phase 2b-1 rejected these (see the removed
// TestValidate_RejectsSketchMeasureInRollup_2b1 /
// TestValidate_RejectsPercentileMeasureInRollup_2b1); phase 2b-2 relaxes the
// restriction now that sketch storage (toolkit hyperloglog/tdigest columns,
// adapters/postgres's rollupTableDDL) exists.
func TestValidate_AllowsSketchMeasureInRollup(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "c", Schema: sketchMeasureSchema("cs", "visitor_id"),
		Metrics: []registry.MetricSpec{{
			Name:     "unique_visitors",
			Source:   "page_viewed",
			Measures: []registry.MetricMeasure{{Kind: "count_distinct", Field: "visitor_id"}},
			Rollup:   &registry.RollupSpec{Bucket: time.Hour},
		}},
	})
	if got := firstErr(r, err); got != nil {
		t.Fatalf("want count_distinct allowed in a Rollup metric, got error: %v", got)
	}

	r2 := registry.New()
	err2 := r2.Register(registry.EntitySpec{
		Name: "d", Schema: sketchMeasureSchema("ds", "duration_ms"),
		Metrics: []registry.MetricSpec{{
			Name:     "latency_p50",
			Source:   "request_completed",
			Measures: []registry.MetricMeasure{{Kind: "percentile", Field: "duration_ms", Percentile: 0.5}},
			Rollup:   &registry.RollupSpec{Bucket: time.Minute},
		}},
	})
	if got := firstErr(r2, err2); got != nil {
		t.Fatalf("want percentile allowed in a Rollup metric, got error: %v", got)
	}
}

// TestMaterializedMetrics asserts MaterializedMetrics returns exactly the
// metrics with a non-nil Rollup, sorted by name, and is empty before Validate
// has run (mirrors Metric's before-Validate contract).
func TestMaterializedMetrics(t *testing.T) {
	r := registry.New()
	if got := r.MaterializedMetrics(); len(got) != 0 {
		t.Fatalf("before Validate: want empty, got %v", got)
	}

	err1 := r.Register(registry.EntitySpec{
		Name: "e", Schema: minimalDynSchema("es"),
		Metrics: []registry.MetricSpec{{
			Name:     "zeta_rollup",
			Source:   "zeta_happened",
			Measures: []registry.MetricMeasure{{Kind: "count"}},
			Rollup:   &registry.RollupSpec{Bucket: time.Hour},
		}},
	})
	err2 := r.Register(registry.EntitySpec{
		Name: "f", Schema: sketchMeasureSchema("fs", "amt"),
		Metrics: []registry.MetricSpec{{
			Name:     "alpha_rollup",
			Source:   "alpha_happened",
			Measures: []registry.MetricMeasure{{Kind: "sum", Field: "amt"}},
			Rollup:   &registry.RollupSpec{Bucket: time.Minute},
		}},
	})
	err3 := r.Register(registry.EntitySpec{
		Name: "g", Schema: minimalDynSchema("gs"),
		Metrics: []registry.MetricSpec{{
			// live-only metric, no Rollup — must NOT appear in MaterializedMetrics.
			Name:     "live_only",
			Source:   "gamma_happened",
			Measures: []registry.MetricMeasure{{Kind: "count"}},
		}},
	})
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatalf("register failed: %v %v %v", err1, err2, err3)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	got := r.MaterializedMetrics()
	if len(got) != 2 {
		t.Fatalf("want 2 materialized metrics, got %d: %+v", len(got), got)
	}
	if got[0].Name != "alpha_rollup" || got[1].Name != "zeta_rollup" {
		t.Fatalf("want sorted [alpha_rollup, zeta_rollup], got [%s, %s]", got[0].Name, got[1].Name)
	}
}
