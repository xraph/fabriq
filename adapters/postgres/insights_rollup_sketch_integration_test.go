//go:build integration

package postgres_test

// TestRollupSketch_* proves (*postgres.InsightsAdapter).Query's stitched
// rollup path for SKETCH measures (Task 8, phase 2b-2): count_distinct via
// timescaledb_toolkit hyperloglog, percentile via tdigest. Unlike the
// additive measures insights_rollup_query_integration_test.go proves EXACT,
// sketch measures are approximate by construction — the assertions here are
// TOLERANCE checks against the adapter's own live-exact ground truth (a twin
// non-materialized metric with the SAME measure, computed via
// COUNT(DISTINCT ...)/percentile_cont directly over fabriq_insights_events),
// not byte-for-byte equality.
//
// Both tests deliberately span the sealed/live-tail WATERMARK (three sealed
// hourly buckets + one still-open bucket, mirroring
// TestRollupQuery_ExactVsLive_Stitched's fixture shape) and query the WHOLE
// range as a single coarser bucket, so the rollup-served result is only
// correct if buildStitchedRollupSQL's sealed hyperloglog/tdigest columns and
// its live-tail hyperloglog/tdigest (built at the SAME rollupHLLRegisters/
// rollupTDigestBuckets size) are actually combined via toolkit `rollup()` in
// the same final GROUP — a bug that dropped or double-counted either side
// would surface as gross error, not just noise within HLL/t-digest's normal
// estimation variance.
//
// Reuses newInsightsHarness (insights_integration_test.go) and the
// queryRows/rollupToFloat helpers insights_rollup_query_integration_test.go
// already defines in this package.

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// sketchOwnerSchema is a placeholder DynamicSchema hosting this file's
// MetricSpec fixtures (registry.Register requires exactly one of
// Model/Schema on every EntitySpec; every non-"count" measure Field must be
// a column of the entity that DECLARES the metric, NOT the metric's Source —
// see rollupQueryOwnerSchema's own doc comment for the same pattern).
// Nothing in this file does CRUD against this entity and no migration ever
// creates its table.
func sketchOwnerSchema() *registry.DynamicSchema {
	return &registry.DynamicSchema{
		Table: "rollupq_sketch_owner",
		Columns: []registry.DynamicColumn{
			{Name: "visitor_id", Type: registry.ColText},
			{Name: "latency_ms", Type: registry.ColFloat},
		},
	}
}

// newSketchRegistry builds a registry carrying two metric pairs, each a
// materialized/live-only twin sourcing the SAME event with IDENTICAL
// measures:
//
//   - "unique_visitors_rollup"/"unique_visitors_live" — a count_distinct
//     measure over "visitor_id" on the "page_viewed" event.
//   - "latency_p95_rollup"/"latency_p95_live" — a p95 percentile measure
//     over "latency_ms" on the "request_completed" event.
//
// Every metric also carries a plain "count" measure alongside its sketch
// measure, so a test can assert the additive measure stays EXACT alongside
// the approximate sketch in the same stitched query.
func newSketchRegistry(t *testing.T, sealGrace, reroll time.Duration) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:   "rollupq_sketch_owner",
		Schema: sketchOwnerSchema(),
		Metrics: []registry.MetricSpec{
			{
				Name:   "unique_visitors_rollup",
				Source: "page_viewed",
				Measures: []registry.MetricMeasure{
					{Kind: "count", As: "n"},
					{Kind: "count_distinct", Field: "visitor_id", As: "uniques"},
				},
				Rollup: &registry.RollupSpec{Bucket: time.Hour, SealGrace: sealGrace, RerollWindow: reroll},
			},
			{
				Name:   "unique_visitors_live",
				Source: "page_viewed",
				Measures: []registry.MetricMeasure{
					{Kind: "count", As: "n"},
					{Kind: "count_distinct", Field: "visitor_id", As: "uniques"},
				},
			},
			{
				Name:   "latency_p95_rollup",
				Source: "request_completed",
				Measures: []registry.MetricMeasure{
					{Kind: "count", As: "n"},
					{Kind: "percentile", Field: "latency_ms", As: "p95", Percentile: 0.95},
				},
				Rollup: &registry.RollupSpec{Bucket: time.Hour, SealGrace: sealGrace, RerollWindow: reroll},
			},
			{
				Name:   "latency_p95_live",
				Source: "request_completed",
				Measures: []registry.MetricMeasure{
					{Kind: "count", As: "n"},
					{Kind: "percentile", Field: "latency_ms", As: "p95", Percentile: 0.95},
				},
			},
		},
	}); err != nil {
		t.Fatalf("register rollupq_sketch_owner entity: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return reg
}

// trackPageView tracks one "page_viewed" event carrying visitorID.
func trackPageView(t *testing.T, a *postgres.Adapter, ctx context.Context, at time.Time, visitorID string) {
	t.Helper()
	if err := a.Track(ctx, []query.AnalyticsEvent{{
		Name:  "page_viewed",
		At:    at,
		Props: map[string]any{"visitor_id": visitorID},
	}}); err != nil {
		t.Fatalf("track page_viewed event at %s visitor_id=%s: %v", at, visitorID, err)
	}
}

// trackLatency tracks one "request_completed" event carrying latencyMs.
func trackLatency(t *testing.T, a *postgres.Adapter, ctx context.Context, at time.Time, latencyMs float64) {
	t.Helper()
	if err := a.Track(ctx, []query.AnalyticsEvent{{
		Name:  "request_completed",
		At:    at,
		Props: map[string]any{"latency_ms": latencyMs},
	}}); err != nil {
		t.Fatalf("track request_completed event at %s latency_ms=%v: %v", at, latencyMs, err)
	}
}

// relativeError returns |got-want|/want, or 0 if want is 0 (avoids a
// divide-by-zero when a fixture's ground truth happens to be exactly zero —
// not expected in these tests, but a defensive definition).
func relativeError(got, want float64) float64 {
	if want == 0 {
		return 0
	}
	return math.Abs(got-want) / math.Abs(want)
}

// TestRollupSketch_CountDistinctWithinTolerance materializes
// "unique_visitors_rollup" (a count_distinct measure over "visitor_id") with
// events spanning three sealed hourly buckets plus a still-open fourth
// bucket, deliberately reusing visitor ids ACROSS buckets (not just within
// one) so a correct answer requires the toolkit's hyperloglog rollup() to
// properly UNION (deduplicate), not merely sum, the sealed + live-tail
// partials. Querying the whole 4h range as one coarser bucket compares the
// rollup-served distinct_count(rollup(hll)) against the live twin's exact
// COUNT(DISTINCT visitor_id) — asserting the estimate is within HLL's
// expected error bound. rollupHLLRegisters=1024 gives a ~1.04/sqrt(1024) ≈
// 3.25% expected standard error; asserting <15% keeps roughly 4 standard
// deviations of headroom (a >4-sigma miss has negligible probability) so
// this test is not flaky while still catching a grossly wrong
// implementation (e.g. one that dropped the live tail, or summed instead of
// unioned, would miss by far more than 15%).
func TestRollupSketch_CountDistinctWithinTolerance(t *testing.T) {
	reg := newSketchRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("unique_visitors_rollup")
	if !ok {
		t.Fatalf("unique_visitors_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC)
	// A pool of 733 (prime) distinct visitor ids. Every bucket cycles
	// through the SAME pool (id = i % distinctPool, eventsPerBucket >
	// distinctPool so each bucket alone already covers the full pool at
	// least once) — all 4 buckets heavily OVERLAP rather than partition the
	// pool, so the true UNIONED distinct count across the whole range is
	// close to distinctPool itself, NOT 4*distinctPool. This specifically
	// exercises hyperloglog rollup()'s deduplicating union across the
	// sealed/live-tail stitch: an implementation that summed per-bucket
	// estimates instead of properly unioning them would land near 4x the
	// true value, far outside any reasonable tolerance.
	const distinctPool = 733
	const eventsPerBucket = 900
	for bucket := 0; bucket < 4; bucket++ {
		bucketStart := base.Add(time.Duration(bucket) * time.Hour)
		for i := 0; i < eventsPerBucket; i++ {
			id := i % distinctPool
			at := bucketStart.Add(time.Duration(i%50) * time.Minute / 50)
			trackPageView(t, a, ctx, at, fmt.Sprintf("v%d", id))
		}
	}

	now := base.Add(3*time.Hour + 10*time.Minute) // 10m into bucket 3 — seals buckets 0-2, leaves 3 open
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}
	wm, wmOK, werr := a.ReadRollupWatermark(ctx, "unique_visitors_rollup")
	if werr != nil {
		t.Fatalf("ReadRollupWatermark: %v", werr)
	}
	if !wmOK || !wm.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("watermark = (%s, %v), want (%s, true) — test fixture must leave bucket 3 open", wm, wmOK, base.Add(3*time.Hour))
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: 4 * time.Hour, From: base, To: base.Add(4 * time.Hour)}

	qRollup := q
	qRollup.Source = "unique_visitors_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "unique_visitors_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) != 1 || len(stitched) != 1 {
		t.Fatalf("expected exactly 1 merged row from each path, got stitched=%d live=%d", len(stitched), len(live))
	}

	exactUniques := rollupToFloat(t, live[0]["uniques"])
	approxUniques := rollupToFloat(t, stitched[0]["uniques"])
	if exactUniques == 0 {
		t.Fatalf("test fixture sanity check failed: live exact distinct count is 0")
	}
	relErr := relativeError(approxUniques, exactUniques)
	t.Logf("count_distinct: exact=%v approx=%v relative_error=%.4f", exactUniques, approxUniques, relErr)
	const tolerance = 0.15
	if relErr >= tolerance {
		t.Fatalf("rollup-served count_distinct out of tolerance: exact=%v approx=%v relative_error=%.4f, want < %v", exactUniques, approxUniques, relErr, tolerance)
	}

	// The plain "count" measure in the SAME metric must stay byte-for-byte
	// exact alongside the approximate sketch measure — additive measures
	// are never affected by a sketch measure sharing the metric.
	exactN := rollupToFloat(t, live[0]["n"])
	gotN := rollupToFloat(t, stitched[0]["n"])
	if gotN != exactN {
		t.Fatalf("additive measure n must stay EXACT alongside the sketch measure: got %v, want %v", gotN, exactN)
	}
}

// TestRollupSketch_PercentileWithinTolerance materializes
// "latency_p95_rollup" (a p95 percentile measure over "latency_ms") with
// events spanning three sealed hourly buckets plus a still-open fourth
// bucket — the same sealed/live-tail shape as the count_distinct test above
// — and compares the rollup-served approx_percentile(rollup(tdigest)) against
// the live twin's exact percentile_cont. t-digest with
// rollupTDigestBuckets=100 buckets is highly accurate for a smooth
// distribution at this sample size, so a tight 5% relative-error tolerance
// still catches a broken stitch (e.g. dropping the live tail, or using
// mismatched toolkit sizes so rollup() silently misbehaves) while easily
// tolerating t-digest's normal estimation noise.
func TestRollupSketch_PercentileWithinTolerance(t *testing.T) {
	reg := newSketchRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("latency_p95_rollup")
	if !ok {
		t.Fatalf("latency_p95_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 2, 4, 0, 0, 0, 0, time.UTC)
	// 400 events per bucket, latency uniformly spread 1..1000ms (a known,
	// smooth distribution so percentile_cont's ground truth is
	// well-defined and t-digest can approximate it tightly).
	const eventsPerBucket = 400
	for bucket := 0; bucket < 4; bucket++ {
		bucketStart := base.Add(time.Duration(bucket) * time.Hour)
		for i := 0; i < eventsPerBucket; i++ {
			latency := float64(1 + (i*997)%1000) // spread across 1..1000ms
			at := bucketStart.Add(time.Duration(i%50) * time.Minute / 50)
			trackLatency(t, a, ctx, at, latency)
		}
	}

	now := base.Add(3*time.Hour + 10*time.Minute)
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}
	wm, wmOK, werr := a.ReadRollupWatermark(ctx, "latency_p95_rollup")
	if werr != nil {
		t.Fatalf("ReadRollupWatermark: %v", werr)
	}
	if !wmOK || !wm.Equal(base.Add(3*time.Hour)) {
		t.Fatalf("watermark = (%s, %v), want (%s, true) — test fixture must leave bucket 3 open", wm, wmOK, base.Add(3*time.Hour))
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: 4 * time.Hour, From: base, To: base.Add(4 * time.Hour)}

	qRollup := q
	qRollup.Source = "latency_p95_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "latency_p95_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) != 1 || len(stitched) != 1 {
		t.Fatalf("expected exactly 1 merged row from each path, got stitched=%d live=%d", len(stitched), len(live))
	}

	exactP95 := rollupToFloat(t, live[0]["p95"])
	approxP95 := rollupToFloat(t, stitched[0]["p95"])
	if exactP95 == 0 {
		t.Fatalf("test fixture sanity check failed: live exact p95 is 0")
	}
	relErr := relativeError(approxP95, exactP95)
	t.Logf("percentile p95: exact=%v approx=%v relative_error=%.4f", exactP95, approxP95, relErr)
	const tolerance = 0.05
	if relErr >= tolerance {
		t.Fatalf("rollup-served p95 out of tolerance: exact=%v approx=%v relative_error=%.4f, want < %v", exactP95, approxP95, relErr, tolerance)
	}

	exactN := rollupToFloat(t, live[0]["n"])
	gotN := rollupToFloat(t, stitched[0]["n"])
	if gotN != exactN {
		t.Fatalf("additive measure n must stay EXACT alongside the sketch measure: got %v, want %v", gotN, exactN)
	}
}
