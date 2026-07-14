package registry_test

import (
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

// TestValidate_RejectsSketchMeasureInRollup_2b1 asserts a Rollup metric
// carrying a non-additive ("sketch") measure — count_distinct or percentile —
// is rejected in phase 2b-1. This restriction is expected to relax once
// sketch storage lands (2b-2).
func TestValidate_RejectsSketchMeasureInRollup_2b1(t *testing.T) {
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
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "count_distinct") {
		t.Fatalf("want sketch-measure-rejected error, got %v", got)
	}
}

// TestValidate_RejectsPercentileMeasureInRollup_2b1 mirrors the count_distinct
// case for the other non-additive Kind, "percentile".
func TestValidate_RejectsPercentileMeasureInRollup_2b1(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "d", Schema: sketchMeasureSchema("ds", "duration_ms"),
		Metrics: []registry.MetricSpec{{
			Name:     "latency_p50",
			Source:   "request_completed",
			Measures: []registry.MetricMeasure{{Kind: "percentile", Field: "duration_ms"}},
			Rollup:   &registry.RollupSpec{Bucket: time.Minute},
		}},
	})
	got := firstErr(r, err)
	if got == nil || !strings.Contains(got.Error(), "percentile") {
		t.Fatalf("want sketch-measure-rejected error, got %v", got)
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
