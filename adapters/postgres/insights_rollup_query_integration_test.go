//go:build integration

package postgres_test

// TestRollupQuery_* proves (*postgres.InsightsAdapter).Query's stitching
// query router (Task 5: rollupCompatible + buildStitchedRollupSQL + the
// routing hook in InsightsAdapter.Query) against a real Postgres: for
// additive measures, a rollup-served (sealed-only or sealed+live-tail
// stitched) result is EXACTLY equal to the fully-live result, never stale,
// never partial. Ground truth throughout is "revenue_live" — a metric
// sourcing the SAME "checkout" event with IDENTICAL measures/dimensions but
// no RollupSpec, so InsightsAdapter.Query always serves it via the
// unaccelerated buildInsightsSQL path (see insights_query_build.go) —
// exactly the comparison the design's Testing section calls for ("ground-
// truthed against the adapter's own live path").
//
// Reuses newInsightsHarness (insights_integration_test.go) and
// MaintainRollup/EnsureRollupTable (Task 4/3) the sibling
// insights_rollup_integration_test.go already exercises directly.

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// rollupQueryOwnerSchema is a placeholder DynamicSchema used only to host
// this file's MetricSpec fixtures via EntitySpec.Metrics (registry.Register
// requires exactly one of Model/Schema on every EntitySpec, and — per
// registry's validateAndBind — every non-"count" measure Field must be a
// column of the entity that DECLARES the metric, NOT the metric's Source;
// see core/registry/rollup_spec_test.go's sketchMeasureSchema for the same
// pattern). Nothing in this file does CRUD against this entity and no
// migration ever creates its table — only the "amount" column exists, so
// the sum/avg/min/max measure Fields below pass that check.
func rollupQueryOwnerSchema() *registry.DynamicSchema {
	return &registry.DynamicSchema{
		Table:   "rollupq_query_owner",
		Columns: []registry.DynamicColumn{{Name: "amount", Type: registry.ColFloat}},
	}
}

// revenueMeasures is the measure set shared by both the materialized
// ("revenue_rollup") and live-only twin ("revenue_live") metric fixtures:
// count + sum + avg + min + max over "amount" — one measure for every
// additive column shape the rollup storage/router supports (avg exercises
// the sum/count decomposition specifically).
func revenueMeasures() []registry.MetricMeasure {
	return []registry.MetricMeasure{
		{Kind: "count", As: "n"},
		{Kind: "sum", Field: "amount", As: "rev"},
		{Kind: "avg", Field: "amount", As: "avgamt"},
		{Kind: "min", Field: "amount", As: "minamt"},
		{Kind: "max", Field: "amount", As: "maxamt"},
	}
}

// newRollupQueryRegistry builds a registry carrying two metrics that source
// the SAME "checkout" event with IDENTICAL measures/dimensions:
//
//   - "revenue_rollup" — materialized (Rollup set from sealGrace/reroll).
//   - "revenue_live" — Rollup left nil, so InsightsAdapter.Query always
//     serves it via the fully-live path — the ground truth every
//     TestRollupQuery_ExactVsLive_* / CoarserBucketReAggregates / AvgExact
//     test below compares the rollup-served result against.
//
// Both metric Names are deliberately DIFFERENT from their shared Source
// ("checkout"): insights.ResolveSource checks reg.Metric(source) BEFORE
// resolveBase, so if a metric's own Name equalled its Source (as the task-4
// fixture's checkoutRollupMetric does), resolving the metric's Source
// (resolveBase(reg, m.Source)) would immediately fail with "a metric's
// Source must be an event ... not another metric". Task 4 never looked its
// fixture up through the registry (MaintainRollup/RollupRange take a
// *registry.MetricSpec directly), so that collision never mattered there;
// it would break this file, which routes through InsightsAdapter.Query ->
// insights.ResolveSource -> reg.Metric(q.Source).
func newRollupQueryRegistry(t *testing.T, sealGrace, reroll time.Duration) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:   "rollupq_owner",
		Schema: rollupQueryOwnerSchema(),
		Metrics: []registry.MetricSpec{
			{
				Name:       "revenue_rollup",
				Source:     "checkout",
				Dimensions: []string{"status"},
				Measures:   revenueMeasures(),
				Rollup:     &registry.RollupSpec{Bucket: time.Hour, SealGrace: sealGrace, RerollWindow: reroll},
			},
			{
				Name:       "revenue_live",
				Source:     "checkout",
				Dimensions: []string{"status"},
				Measures:   revenueMeasures(),
			},
		},
	}); err != nil {
		t.Fatalf("register rollupq_owner entity: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return reg
}

// trackRevenue tracks one "checkout" event at (at, status, amount) under
// ctx, via the app-role adapter a.
func trackRevenue(t *testing.T, a *postgres.Adapter, ctx context.Context, at time.Time, status string, amount float64) {
	t.Helper()
	if err := a.Track(ctx, []query.AnalyticsEvent{{
		Name:  "checkout",
		At:    at,
		Props: map[string]any{"status": status, "amount": amount},
	}}); err != nil {
		t.Fatalf("track checkout event at %s status=%s amount=%v: %v", at, status, amount, err)
	}
}

// queryRows runs q via ia.Query and returns the scanned rows, failing the
// test on error. InsightsAdapter.Query only supports *[]map[string]any as
// its destination (assignMapsDest, adapter.go) — the same dynamic-row shape
// the admin raw-query console consumes.
func queryRows(t *testing.T, ia *postgres.InsightsAdapter, ctx context.Context, q query.AnalyticsQuery) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if err := ia.Query(ctx, q, &rows); err != nil {
		t.Fatalf("Query(Source=%q): %v", q.Source, err)
	}
	return rows
}

// rollupToFloat coerces a scanned measure value to float64, mirroring core/
// insights's unexported toFloatT (conformance.go) — duplicated here (not
// imported: adapters/postgres does not depend on core/insights' test-only
// helpers) because a real Postgres map-scan can hand a NUMERIC column back
// as string/[]byte/a driver-specific decimal type implementing
// json.Marshaler, depending on the scan-plan chosen for an `any`
// destination, not just the plain Go-numeric shapes.
func rollupToFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err != nil {
			t.Fatalf("rollupToFloat: parse %q: %v", n, err)
		}
		return f
	case []byte:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(n)), 64)
		if err != nil {
			t.Fatalf("rollupToFloat: parse %q: %v", n, err)
		}
		return f
	default:
		if m, ok := v.(interface{ MarshalJSON() ([]byte, error) }); ok {
			buf, err := m.MarshalJSON()
			if err != nil {
				t.Fatalf("rollupToFloat: MarshalJSON: %v", err)
			}
			f, perr := strconv.ParseFloat(strings.TrimSpace(string(buf)), 64)
			if perr != nil {
				t.Fatalf("rollupToFloat: parse %q: %v", string(buf), perr)
			}
			return f
		}
		t.Fatalf("rollupToFloat: unsupported type %T (%v)", v, v)
		return 0
	}
}

// sortRowsByBucketAndStatus sorts cube-query result rows deterministically
// (bucket, then status) so two independently-produced result sets — the
// rollup-served rows and the fully-live ground truth — can be compared
// row-by-row regardless of any incidental ordering difference between the
// two query paths (both actually default-order the same way, but sorting
// here removes any doubt).
func sortRowsByBucketAndStatus(rows []map[string]any) {
	sort.Slice(rows, func(i, j int) bool {
		bi, _ := rows[i]["bucket"].(time.Time)
		bj, _ := rows[j]["bucket"].(time.Time)
		if !bi.Equal(bj) {
			return bi.Before(bj)
		}
		si, _ := rows[i]["status"].(string)
		sj, _ := rows[j]["status"].(string)
		return si < sj
	})
}

// assertRowsExactlyEqual asserts got and want have the same length and, at
// each (sorted) position, an identical bucket/status key and byte-for-byte
// equal (via rollupToFloat) n/rev/avgamt/minamt/maxamt — the additive-
// exactness invariant every TestRollupQuery_ExactVsLive_*,
// CoarserBucketReAggregates, and AvgExact test below proves.
func assertRowsExactlyEqual(t *testing.T, got, want []map[string]any) {
	t.Helper()
	sortRowsByBucketAndStatus(got)
	sortRowsByBucketAndStatus(want)
	if len(got) != len(want) {
		t.Fatalf("row count mismatch: got %d, want %d\ngot=%+v\nwant=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		gb, _ := g["bucket"].(time.Time)
		wb, _ := w["bucket"].(time.Time)
		if !gb.Equal(wb) {
			t.Fatalf("row %d: bucket mismatch: got %s, want %s", i, gb, wb)
		}
		if g["status"] != w["status"] {
			t.Fatalf("row %d: status mismatch: got %v, want %v", i, g["status"], w["status"])
		}
		for _, col := range []string{"n", "rev", "avgamt", "minamt", "maxamt"} {
			gf := rollupToFloat(t, g[col])
			wf := rollupToFloat(t, w[col])
			if gf != wf {
				t.Fatalf("row %d (bucket=%s status=%v): %s mismatch: got %v, want %v\ngot row=%+v\nwant row=%+v",
					i, gb, g["status"], col, gf, wf, g, w)
			}
		}
	}
}

// TestRollupQuery_ExactVsLive_Sealed materializes "revenue_rollup" with
// events entirely inside three sealed hourly buckets, runs MaintainRollup so
// all three are rolled up, then asserts Query(Source: "revenue_rollup") over
// the whole range is byte-for-byte identical to Query(Source: "revenue_live")
// — additive exactness, fully-sealed (the live-tail CTE inside the stitched
// query contributes zero rows here, since q.To == the watermark).
func TestRollupQuery_ExactVsLive_Sealed(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 6, 8, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)
		trackRevenue(t, a, ctx, bucketStart.Add(10*time.Minute), "ok", 10+float64(i))
		trackRevenue(t, a, ctx, bucketStart.Add(20*time.Minute), "ok", 20+float64(i)*2)
		trackRevenue(t, a, ctx, bucketStart.Add(30*time.Minute), "err", 5+float64(i))
	}

	now := base.Add(3*time.Hour + 5*time.Minute) // well past SealGrace for all 3 buckets
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: time.Hour, From: base, To: base.Add(3 * time.Hour)}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) == 0 {
		t.Fatalf("test fixture produced no live rows — nothing was actually compared")
	}
	assertRowsExactlyEqual(t, stitched, live)
}

// TestRollupQuery_ExactVsLive_Stitched is the correctness core: events land
// in three sealed hourly buckets AND the current, still-open bucket;
// MaintainRollup seals only the first three. Querying the WHOLE range
// (sealed buckets + the open one) must be byte-for-byte identical to the
// fully-live result — proving the sealed-rollup + live-tail stitch produces
// EXACTLY the same answer as computing everything live, which is the
// design's central correctness invariant ("D5 — Stitching router").
func TestRollupQuery_ExactVsLive_Stitched(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC)
	// Buckets 0, 1, 2 will be sealed; bucket 3 is the current, open bucket.
	for i := 0; i < 3; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)
		trackRevenue(t, a, ctx, bucketStart.Add(10*time.Minute), "ok", 10+float64(i))
		trackRevenue(t, a, ctx, bucketStart.Add(45*time.Minute), "err", 3+float64(i))
	}
	openBucket := base.Add(3 * time.Hour)
	trackRevenue(t, a, ctx, openBucket.Add(5*time.Minute), "ok", 77)
	trackRevenue(t, a, ctx, openBucket.Add(6*time.Minute), "ok", 23)
	trackRevenue(t, a, ctx, openBucket.Add(7*time.Minute), "err", 9)

	now := base.Add(3*time.Hour + 10*time.Minute) // 10m into bucket 3 — past 1m SealGrace for buckets 0-2, not for bucket 3
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	// Sanity: the watermark must have advanced to exactly the boundary
	// between the sealed range and the open bucket, or this test isn't
	// actually exercising the stitch (sealed + live-tail) at all.
	wm, wmOK, werr := a.ReadRollupWatermark(ctx, "revenue_rollup")
	if werr != nil {
		t.Fatalf("ReadRollupWatermark: %v", werr)
	}
	if !wmOK || !wm.Equal(openBucket) {
		t.Fatalf("watermark = (%s, %v), want (%s, true) — test fixture must leave bucket 3 open", wm, wmOK, openBucket)
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: time.Hour, From: base, To: base.Add(4 * time.Hour)}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) == 0 {
		t.Fatalf("test fixture produced no live rows — nothing was actually compared")
	}
	assertRowsExactlyEqual(t, stitched, live)
}

// TestRollupQuery_CoarserBucketReAggregates seals four hourly buckets (grain
// = m.Rollup.Bucket = 1h) and queries with TimeBucket = 4h — a clean whole
// multiple of the rollup grain, so rollupCompatible allows it — asserting
// the coarser re-bucketed/re-aggregated stitched result equals live daily-
// style aggregation at the SAME coarser grain (buildInsightsSQL can bucket
// raw events at any grain, so "revenue_live" with TimeBucket=4h is exactly
// the ground truth to compare against).
func TestRollupQuery_CoarserBucketReAggregates(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)
		trackRevenue(t, a, ctx, bucketStart.Add(12*time.Minute), "ok", 10*float64(i+1))
		trackRevenue(t, a, ctx, bucketStart.Add(48*time.Minute), "err", 2*float64(i+1))
	}

	now := base.Add(4*time.Hour + 5*time.Minute)
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: 4 * time.Hour, From: base, To: base.Add(4 * time.Hour)}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) == 0 {
		t.Fatalf("test fixture produced no live rows — nothing was actually compared")
	}
	if len(live) != 2 { // one bucket, two statuses
		t.Fatalf("expected exactly 2 rows (one 4h bucket x 2 statuses), got %d: %+v", len(live), live)
	}
	assertRowsExactlyEqual(t, stitched, live)
}

// TestRollupQuery_AvgExact isolates the avg measure with deliberately
// skewed per-bucket counts, so a bug that averaged the per-hour averages
// together (rather than combining sum/count and dividing once) would
// diverge from the correct weighted mean. Bucket 0 has ONE event (amount
// 100, avg=100); bucket 1 has THREE events (amount 10,10,10, avg=10).
// Averaging the two per-bucket averages would give (100+10)/2 = 55; the
// correct weighted average over the merged 2h bucket is
// (100+10+10+10)/4 = 32.5. Comparing against "revenue_live" (which computes
// AVG() directly over the raw rows, unambiguously correct) proves
// buildStitchedRollupSQL's sum/count decomposition + final division lands
// on the correct weighted value.
func TestRollupQuery_AvgExact(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 9, 0, 0, 0, 0, time.UTC)
	trackRevenue(t, a, ctx, base.Add(10*time.Minute), "ok", 100) // bucket 0: n=1, avg=100
	bucket1 := base.Add(time.Hour)
	trackRevenue(t, a, ctx, bucket1.Add(10*time.Minute), "ok", 10) // bucket 1: n=3, avg=10
	trackRevenue(t, a, ctx, bucket1.Add(20*time.Minute), "ok", 10)
	trackRevenue(t, a, ctx, bucket1.Add(30*time.Minute), "ok", 10)

	now := base.Add(2*time.Hour + 5*time.Minute)
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	ia := postgres.NewInsightsAdapter(a)
	// TimeBucket=2h merges both hourly buckets into one output row.
	q := query.AnalyticsQuery{TimeBucket: 2 * time.Hour, From: base, To: base.Add(2 * time.Hour)}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	stitched := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(stitched) != 1 || len(live) != 1 {
		t.Fatalf("expected exactly 1 merged row from each path, got stitched=%d live=%d", len(stitched), len(live))
	}
	wantAvg := 32.5
	gotLiveAvg := rollupToFloat(t, live[0]["avgamt"])
	if gotLiveAvg != wantAvg {
		t.Fatalf("test fixture sanity check failed: live avg = %v, want %v (fixture math is wrong, not the code under test)", gotLiveAvg, wantAvg)
	}
	assertRowsExactlyEqual(t, stitched, live)
}

// TestRollupQuery_IncompatibleFallsBackToLive queries "revenue_rollup" (a
// materialized metric) with a Filter over "amount" — a measure field, not a
// declared Dimension — so rollupCompatible must return false and
// InsightsAdapter.Query must fall back to the fully-live buildInsightsSQL
// path. Asserts the result is still correct (identical to "revenue_live"
// with the SAME filter): an incompatible query must never silently ignore
// the filter or read stale/partial data from the rollup.
func TestRollupQuery_IncompatibleFallsBackToLive(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		bucketStart := base.Add(time.Duration(i) * time.Hour)
		trackRevenue(t, a, ctx, bucketStart.Add(10*time.Minute), "ok", 30+float64(i)) // <= 50, excluded by the filter
		trackRevenue(t, a, ctx, bucketStart.Add(20*time.Minute), "ok", 80+float64(i))
		trackRevenue(t, a, ctx, bucketStart.Add(30*time.Minute), "err", 60+float64(i))
	}

	now := base.Add(2*time.Hour + 5*time.Minute)
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{
		TimeBucket: time.Hour,
		From:       base,
		To:         base.Add(2 * time.Hour),
		Filter:     query.Where{query.Gt("amount", 50)}, // "amount" is a measure, not a Dimension
	}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	fellBack := queryRows(t, ia, ctx, qRollup)

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) == 0 {
		t.Fatalf("test fixture produced no live rows — nothing was actually compared")
	}
	assertRowsExactlyEqual(t, fellBack, live)
}

// TestRollupQuery_ReadsRollupWhenCompatible proves a rollup-compatible query
// actually READS the rollup table rather than merely happening to compute
// the same answer live: after sealing a bucket via MaintainRollup, its
// rollup row is overwritten (via the owner/superuser connection) with a
// sentinel "rev" value the live "checkout" events could never produce.
// Querying "revenue_rollup" over that bucket must return the SENTINEL
// value, not the true live sum — the only way that can happen is if the
// stitched query actually selected from fabriq_insights_rollup_revenue_rollup.
func TestRollupQuery_ReadsRollupWhenCompatible(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, owner := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	m, ok := reg.Metric("revenue_rollup")
	if !ok {
		t.Fatalf("revenue_rollup metric not found in registry")
	}
	if err := owner.EnsureRollupTable(context.Background(), m); err != nil {
		t.Fatalf("EnsureRollupTable: %v", err)
	}

	base := time.Date(2025, 1, 11, 6, 0, 0, 0, time.UTC)
	trackRevenue(t, a, ctx, base.Add(10*time.Minute), "ok", 10) // true rev for this bucket/status = 10

	now := base.Add(1*time.Hour + 5*time.Minute)
	if err := a.MaintainRollup(ctx, m, now); err != nil {
		t.Fatalf("MaintainRollup: %v", err)
	}

	const sentinel = 999999.0
	if _, err := owner.Driver().Exec(context.Background(),
		`UPDATE fabriq_insights_rollup_revenue_rollup SET rev = $1 WHERE tenant_id = $2 AND scope_id IS NULL AND bucket_start = $3 AND status = $4`,
		sentinel, "acme", base, "ok"); err != nil {
		t.Fatalf("plant sentinel rollup row: %v", err)
	}

	ia := postgres.NewInsightsAdapter(a)
	rows := queryRows(t, ia, ctx, query.AnalyticsQuery{
		Source:     "revenue_rollup",
		TimeBucket: time.Hour,
		From:       base,
		To:         base.Add(time.Hour), // entirely inside the sealed range — live tail is empty
	})
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row, got %d: %+v", len(rows), rows)
	}
	got := rollupToFloat(t, rows[0]["rev"])
	if got != sentinel {
		t.Fatalf("rev = %v, want the planted sentinel %v — query did not read the rollup table", got, sentinel)
	}
}

// TestRollupQuery_NoWatermarkFallsBackToLive is the review-fix regression
// test (phase 2b Task 5 fix): "revenue_rollup" is materialized
// (MetricSpec.Rollup != nil) and its query shape is rollupCompatible, but
// NEITHER EnsureRollupTable NOR MaintainRollup is ever called here — so
// fabriq_insights_rollup_revenue_rollup does not exist AND
// fabriq_insights_rollup_state has no row for it (ReadRollupWatermark
// returns hasWatermark=false, not an error — ReadRollupWatermark only reads
// the always-migrated state table, never the per-metric one). Before the
// fix, InsightsAdapter.Query routed to buildStitchedRollupSQL regardless,
// whose sealed CTE does `FROM fabriq_insights_rollup_revenue_rollup`
// unconditionally — Postgres needs that relation to exist at PLAN time even
// though the WHERE clause degrades to "AND FALSE" when hasWatermark is
// false, so the query failed with "relation ... does not exist" the moment
// a MetricSpec declared Rollup, well before any maintainer pass ever ran.
// After the fix, Query must fall through to the ordinary live path
// (buildInsightsSQL) instead, returning the correct live-computed result
// with no error. Ground truth is "revenue_live" — the same events, same
// measures/dimensions, un-materialized twin — exactly as every other test
// in this file compares against.
func TestRollupQuery_NoWatermarkFallsBackToLive(t *testing.T) {
	reg := newRollupQueryRegistry(t, time.Minute, 2*time.Hour)
	a, _ := newInsightsHarness(t, reg)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Deliberately skip EnsureRollupTable/MaintainRollup: no per-metric
	// rollup table, no watermark row.
	base := time.Date(2025, 1, 12, 9, 0, 0, 0, time.UTC)
	trackRevenue(t, a, ctx, base.Add(10*time.Minute), "ok", 42)
	trackRevenue(t, a, ctx, base.Add(20*time.Minute), "ok", 8)
	trackRevenue(t, a, ctx, base.Add(30*time.Minute), "err", 5)

	ia := postgres.NewInsightsAdapter(a)
	q := query.AnalyticsQuery{TimeBucket: time.Hour, From: base, To: base.Add(time.Hour)}

	qRollup := q
	qRollup.Source = "revenue_rollup"
	fellBack := queryRows(t, ia, ctx, qRollup) // must NOT error with "relation ... does not exist"

	qLive := q
	qLive.Source = "revenue_live"
	live := queryRows(t, ia, ctx, qLive)

	if len(live) == 0 {
		t.Fatalf("test fixture produced no live rows — nothing was actually compared")
	}
	assertRowsExactlyEqual(t, fellBack, live)
}
