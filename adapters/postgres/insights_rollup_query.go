package postgres

// The stitching query router (phase 2b, Task 5; sketch measures added Task
// 8/2b-2): when a cube Query targets a materialized metric
// (MetricSpec.Rollup != nil) and the query's shape is "rollup-compatible"
// (rollupCompatible, below), InsightsAdapter.Query serves it by combining the
// SEALED rollup table with a LIVE tail computed directly from
// fabriq_insights_events — buildStitchedRollupSQL builds that combined query.
// Correctness invariant: for additive measures (count/sum/avg/min/max), the
// stitched result is EXACTLY equal to the fully-live result, never stale (the
// live tail always covers events up to `to`, so nothing sealed-but-not-yet-
// rolled-up or arrived-since is ever missing) and never partial. For sketch
// measures (count_distinct/percentile, phase 2b-2), the stitched result is
// APPROXIMATE by construction — combining a sealed hyperloglog/tdigest with a
// live-tail one built at the SAME toolkit size (rollupHLLRegisters/
// rollupTDigestBuckets, insights_rollup_ddl.go) via `rollup()` and reading it
// with `distinct_count`/`approx_percentile` — never stale or partial in the
// same range sense as the additive case, just not exact; this is the
// explicit trade a caller accepts by declaring a sketch measure on a Rollup
// metric (see the design's "Sketches (2b-2)" section). An incompatible query
// (see rollupCompatible) is not routed here at all — the caller falls back
// to buildInsightsSQL, which is always exact by construction (both additive
// and sketch measures).
//
// The watermark/seal-boundary tiling this file depends on (see
// insights_rollup_maintain.go's MaintainRollup/AdvanceRollupWatermark):
// fabriq_insights_rollup_state.watermark_bucket is the EXCLUSIVE upper bound
// of sealed bucket_start values — i.e. every rollup row with bucket_start <
// watermark is sealed and up to date, and the bucket starting AT watermark
// itself is NOT yet in the rollup (it may still be open, or sealed-but-not-
// yet-rolled-up by the next maintainer pass).
//
// q.From/q.To are NOT guaranteed to be grain-aligned (a caller may ask for
// "the last 90 minutes" against an hourly rollup), but every rollup
// bucket_start value IS grain-aligned. So the sealed/live boundary cannot
// simply be "watermark" compared against raw q.From/q.To — that would either
// drop the leading partial bucket [q.From, ceilToGrain(q.From)) (sealed's
// bucket_start >= q.From, raw, excludes the sealed bucket q.From falls
// inside; live's lower bound starts AT watermark, well past it) or
// double-count a trailing partial bucket, whenever q.From/q.To land
// mid-bucket. Instead this file computes two grain-ALIGNED boundaries
// (sealedLo, sealedHiExcl, below) and tiles sealed/live around THOSE:
//
//   - sealedLo = ceilToGrain(q.From, g) — the first rollup bucket_start
//     fully inside [q.From, ∞): rounds UP, so a mid-bucket q.From excludes
//     that partial bucket from the sealed side (it belongs to live instead).
//   - sealedHiExcl = min(watermark, floorToGrain(q.To, g)) when q.To is
//     bounded, else just `watermark` — the sealed CTE serves bucket_start ∈
//     [sealedLo, sealedHiExcl), i.e. only buckets BOTH sealed (before the
//     watermark) AND fully before q.To (floorToGrain rounds DOWN, so a
//     mid-bucket q.To excludes that partial bucket from the sealed side
//     too).
//   - the live CTE covers everything sealed does NOT: at ∈ [q.From, q.To)
//     AND (at < sealedLo OR at >= sealedHiExcl) — the leading partial
//     [q.From, sealedLo), the trailing partial + unsealed tail
//     [sealedHiExcl, q.To), and nothing sealed already served.
//
// sealed ∪ live tiles [q.From, q.To) with NO gap and NO overlap by
// construction (sealedLo/sealedHiExcl are the shared boundary on both
// sides), which is the numeric crux of "stitched == fully-live" even when
// q.From/q.To are not clean multiples of the rollup grain.

import (
	"fmt"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// floorToGrain rounds t DOWN to the nearest multiple of g. Pure, no I/O.
//
// Uses time.Time.Truncate, which aligns to Go's zero-time origin (an
// absolute, epoch-independent instant) — this matches Postgres/TimescaleDB's
// time_bucket(interval, ts) alignment for any grain that evenly divides a
// calendar day (minute, hour, or day itself): TimescaleDB's default
// time_bucket origin (2000-01-03, a Monday, chosen to align weekly buckets)
// is itself exactly midnight-aligned, and the gap between that origin and
// Go's zero time is a whole number of days — so truncating to a sub-day
// grain lands on the identical boundary either way. It would NOT hold for a
// weekly (or coarser, non-day-multiple) grain, but rollupCompatible's
// bucket-multiple rule only ever routes practical hour/day-scale grains
// through this file; the unaligned-boundary regression test
// (TestRollupQuery_UnalignedBoundaryExactVsLive) is the oracle that would
// catch a mismatch if that assumption ever stopped holding.
func floorToGrain(t time.Time, g time.Duration) time.Time {
	if g <= 0 {
		return t
	}
	return t.Truncate(g)
}

// ceilToGrain rounds t UP to the nearest multiple of g (t itself, unchanged,
// if it is already grain-aligned). Pure, no I/O. See floorToGrain's doc for
// the origin-alignment argument this shares.
func ceilToGrain(t time.Time, g time.Duration) time.Time {
	if g <= 0 {
		return t
	}
	fl := t.Truncate(g)
	if fl.Equal(t) {
		return fl
	}
	return fl.Add(g)
}

// rollupCompatible reports whether q can be served by m's materialized
// rollup instead of the fully-live path. PURE — no I/O, no db/registry
// access beyond the passed-in MetricSpec. ALL of the following must hold; if
// any fails the caller must use the fully-live path (buildInsightsSQL),
// which is always correct:
//
//   - m.Rollup is non-nil and its Bucket is > 0 (m is actually materialized).
//   - q.TimeBucket is a POSITIVE INTEGER MULTIPLE of m.Rollup.Bucket: a
//     rollup stored at, say, 1h grain can serve a query bucketed at 1h, 2h,
//     6h, 24h (all cleanly re-bucketable from hourly partials, because every
//     coarser boundary that is a whole multiple of the rollup grain is also
//     one of the rollup's own bucket boundaries — time_bucket in Postgres
//     aligns to the epoch, so this holds for ANY clean multiple, not just
//     calendar-aligned ones) but NOT a query bucketed FINER than the rollup
//     grain (the rollup has already discarded sub-bucket resolution) and
//     NOT a bucket that is not a clean multiple (its boundaries would not
//     line up with the rollup's own bucket_start values).
//   - every dimension q asks for is one the rollup stores (q.Dimensions ⊆
//     m.Dimensions) — the rollup table has no column for a dimension
//     outside the metric's declared set.
//   - every Filter condition's column (recursively through OR groups) is one
//     of m.Dimensions — a filter on a non-dimension prop (a measure field,
//     or an ad-hoc prop the metric never declared) cannot be evaluated
//     against already-aggregated rows: that prop's per-event values no
//     longer exist once rolled up.
//
// Having/OrderBy/Limit/Offset impose NO restriction here: Having applies to
// the measure aliases the combined query still produces (buildStitchedRollupSQL
// re-derives the same aliasExpr map mapHavingCond needs); OrderBy/Limit/
// Offset apply to the final result rows exactly as they do for the live path.
func rollupCompatible(q query.AnalyticsQuery, m *registry.MetricSpec) bool {
	if m == nil || m.Rollup == nil || m.Rollup.Bucket <= 0 {
		return false
	}
	if q.TimeBucket <= 0 || q.TimeBucket < m.Rollup.Bucket || q.TimeBucket%m.Rollup.Bucket != 0 {
		return false
	}

	dimSet := make(map[string]bool, len(m.Dimensions))
	for _, d := range m.Dimensions {
		dimSet[d] = true
	}
	for _, d := range q.Dimensions {
		if !dimSet[d] {
			return false
		}
	}

	if err := query.ValidateConds(q.Filter, func(c string) bool { return dimSet[c] }); err != nil {
		return false
	}
	return true
}

// rollupMeasurePartial describes how one query.Measure's partial is carried
// through the sealed/live-tail "parts" shape and re-aggregated in the final
// SELECT: count/sum/min/max store ONE additive partial column (named by the
// measure's output alias, matching the rollup table's own column exactly —
// see rollupMeasureColumns, insights_rollup_ddl.go); avg is decomposed into
// TWO partial columns ("<alias>__sum", "<alias>__count"), mirroring
// rollupMeasureSelect (insights_rollup_maintain.go) — additive storage, so
// avg is recomputed as sum/count only once, in the final SELECT.
// count_distinct/percentile ("sketch" measures, phase 2b-2) also store ONE
// partial column, but it carries a toolkit hyperloglog/tdigest VALUE (not a
// plain additive number) — the final SELECT combines sealed+live-tail
// partials via `rollup()` rather than sum/min/max. percentile is set (the
// measure's requested fraction) only for MeasurePercentile; ignored
// otherwise, mirroring query.Measure.Percentile itself.
type rollupMeasurePartial struct {
	kind       query.MeasureKind
	field      string
	alias      string
	cols       []string
	percentile float64
}

// planRollupMeasures validates each measure's output alias and computes its
// partial-column plan. count/sum/avg/min/max are additive; count_distinct/
// percentile are sketch-based (phase 2b-2, combined via toolkit rollup()) —
// together these are every kind registry.Validate permits on a Rollup-opted
// MetricSpec. Any other kind here indicates the registry validation gate was
// bypassed (a metric was hand-built rather than validated); reject rather
// than silently building nonsense SQL for it.
func planRollupMeasures(measures []query.Measure) ([]rollupMeasurePartial, error) {
	out := make([]rollupMeasurePartial, 0, len(measures))
	for _, meas := range measures {
		alias, err := measureAlias(meas)
		if err != nil {
			return nil, err
		}
		var cols []string
		var percentile float64
		switch meas.Kind {
		case query.MeasureCount, query.MeasureSum, query.MeasureMin, query.MeasureMax, query.MeasureCountDistinct:
			cols = []string{alias}
		case query.MeasureAvg:
			sumCol, countCol := alias+"__sum", alias+"__count"
			if !insightsIdentRe.MatchString(sumCol) || !insightsIdentRe.MatchString(countCol) {
				return nil, fmt.Errorf("fabriq: invalid decomposed avg column for alias %q", alias)
			}
			cols = []string{sumCol, countCol}
		case query.MeasurePercentile:
			if !(meas.Percentile > 0 && meas.Percentile < 1) {
				return nil, fmt.Errorf("fabriq: percentile must be in (0,1), got %v", meas.Percentile)
			}
			cols = []string{alias}
			percentile = meas.Percentile
		default:
			return nil, fmt.Errorf("fabriq: measure kind %q is not servable from a rollup", meas.Kind)
		}
		out = append(out, rollupMeasurePartial{kind: meas.Kind, field: meas.Field, alias: alias, cols: cols, percentile: percentile})
	}
	return out, nil
}

// mapCondToRollupDim renders one filter condition against the rollup table's
// plain TEXT dimension column, the sealed-CTE sibling of mapCondToProp
// (insights_query_build.go), which renders the SAME condition against a
// JSONB prop accessor for the live-tail CTE. Column is quoted directly (a
// real column reference), not run through propAccessor's `col ->> 'key'`
// idiom. Mirrors mapCondToProp's operator handling and its numeric-cast rule
// for Gt/Gte/Lt/Lte (the rollup dimension column is TEXT, exactly like a
// JSONB accessor's implicit result type, so a numeric-valued bound needs the
// same ::numeric cast to compare correctly rather than lexicographically) so
// the sealed and live-tail WHERE clauses filter identically — a requirement
// for "stitched == fully-live".
//
// allowed, when non-nil, restricts which columns may appear (the rollup's
// declared Dimensions) — rollupCompatible has already guaranteed every
// Filter condition names a rollup dimension before this is ever reached, but
// this is the injection-guard boundary that enforces it at the SQL-building
// layer too, independent of but consistent with mapCondToProp's own allowed
// map.
func mapCondToRollupDim(c query.Cond, argN *int, allowed map[string]bool) (frag string, args []any, err error) {
	if c.IsGroup() {
		if len(c.Or) == 0 {
			return "", nil, fmt.Errorf("fabriq: empty OR group in rollup filter")
		}
		var parts []string
		for _, sub := range c.Or {
			f, a, serr := mapCondToRollupDim(sub, argN, allowed)
			if serr != nil {
				return "", nil, serr
			}
			parts = append(parts, f)
			args = append(args, a...)
		}
		return "(" + strings.Join(parts, " OR ") + ")", args, nil
	}

	if !insightsIdentRe.MatchString(c.Column) {
		return "", nil, fmt.Errorf("fabriq: invalid rollup dimension %q", c.Column)
	}
	if allowed != nil && !allowed[c.Column] {
		return "", nil, fmt.Errorf("fabriq: filter column %q is not a rollup dimension", c.Column)
	}
	col := fmt.Sprintf("%q", c.Column)
	ph := func() string {
		p := fmt.Sprintf("$%d", *argN)
		*argN++
		return p
	}
	switch c.Op {
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		cc := col
		if isNumericValue(c.Value) {
			cc = "(" + col + ")::numeric"
		}
		return fmt.Sprintf("%s %s %s", cc, sqlOp[c.Op], ph()), []any{c.Value}, nil
	case query.OpEq, query.OpNe, query.OpLike, query.OpILike:
		return fmt.Sprintf("%s %s %s", col, sqlOp[c.Op], ph()), []any{c.Value}, nil
	case query.OpIn:
		return fmt.Sprintf("%s = ANY(%s)", col, ph()), []any{c.Value}, nil
	case query.OpNotIn:
		return fmt.Sprintf("NOT (%s = ANY(%s))", col, ph()), []any{c.Value}, nil
	case query.OpIsNull:
		return fmt.Sprintf("%s IS NULL", col), nil, nil
	case query.OpIsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", col), nil, nil
	default:
		return "", nil, fmt.Errorf("fabriq: unsupported filter operator %q", c.Op)
	}
}

// buildStitchedRollupSQL builds the stitched (sealed-rollup + live-tail)
// aggregation SQL and its positional args for query q against materialized
// metric m, scoped to tenant tid. measures/dims/bucket are the EFFECTIVE
// query shape (insights.EffectiveQuery's output for q against m's
// Descriptor) — dims may be a subset of m.Dimensions (rollupCompatible
// allows q.Dimensions ⊆ m.Dimensions); bucket is q's requested TimeBucket
// (or m's DefaultBucket), already proven by rollupCompatible to be a clean
// multiple of m.Rollup.Bucket.
//
// watermark/hasWatermark come from ReadRollupWatermark: hasWatermark==false
// means the metric has never had a maintainer pass (no sealed data at all)
// — the sealed CTE is built to always return zero rows (an unconditional
// "AND FALSE"), so the ENTIRE query range is served live, which is still
// exactly correct, just not accelerated.
//
// Structure (see the design's §"The stitching router" and this file's
// header comment for the sealed/live boundary tiling):
//
//	WITH sealed AS (
//	  SELECT bucket_start, scope_id, <m.Dimensions...>, <partial cols...>
//	  FROM fabriq_insights_rollup_<metric>
//	  WHERE tenant_id = $1 AND bucket_start >= $from AND bucket_start < $sealedHi
//	    [AND <dim filters, plain TEXT columns>]
//	), live AS (
//	  SELECT time_bucket($grain::interval, at) AS bucket_start, scope_id,
//	         <props->>dim AS dim...>,
//	         count(*)::numeric AS <cnt>, sum((props->>f)::numeric) AS <sum>,
//	         min((props->>f)::numeric) AS <min>, max((props->>f)::numeric) AS <max>,
//	         sum((props->>f)::numeric) AS <avg>__sum, count(props->>f)::numeric AS <avg>__count
//	  FROM fabriq_insights_events
//	  WHERE tenant_id = $1 AND name = $src AND at >= $liveLo AND at < $liveHi
//	    [AND <dim filters, JSONB accessor>]
//	  GROUP BY bucket_start, scope_id, <dim aliases...>
//	), parts AS (SELECT * FROM sealed UNION ALL SELECT * FROM live)
//	SELECT time_bucket($reqGrain::interval, bucket_start) AS bucket, <requested dims...>,
//	       sum(<cnt>) AS <alias>, sum(<sum>) AS <alias>, min(<min>) AS <alias>, max(<max>) AS <alias>,
//	       (sum(<avg>__sum) / NULLIF(sum(<avg>__count), 0)) AS <alias>
//	FROM parts
//	GROUP BY bucket, <requested dims...>
//	[HAVING ...] [ORDER BY ...] [LIMIT ...] [OFFSET ...]
//
// scope_id is carried through sealed/live/parts (so RLS, which already
// filtered both source tables to the caller's visible scope(s), stays
// correct) but deliberately NOT included in the final GROUP BY — matching
// buildInsightsSQL's own live path, which never groups by scope_id either:
// a scoped reader's visible rows are already exactly the ones RLS let
// through, so summing across them in the final aggregation is the same
// "aggregate over every visible scope" semantics the live path has always
// had, not a new behavior this router introduces.
//
// Every interpolated identifier (dimension names, measure aliases/partial
// columns) is insightsIdentRe-checked before being placed in the SQL text;
// every value (tenant, bounds, filter values, Having values, grains) travels
// as a bound $N parameter — the same injection-guard contract
// insights_query_build.go documents for buildInsightsSQL.
func (a *Adapter) buildStitchedRollupSQL(q query.AnalyticsQuery, tid string, m *registry.MetricSpec, measures []query.Measure, dims []string, bucket time.Duration, watermark time.Time, hasWatermark bool) (sql string, args []any, err error) {
	if m.Rollup == nil || m.Rollup.Bucket <= 0 {
		return "", nil, fmt.Errorf("fabriq: metric %q has no rollup configured", m.Name)
	}
	if bucket <= 0 {
		return "", nil, fmt.Errorf("fabriq: stitched rollup query requires a positive TimeBucket")
	}
	if len(measures) == 0 {
		return "", nil, fmt.Errorf("fabriq: insights query needs at least one measure")
	}
	table, err := rollupTableName(m.Name)
	if err != nil {
		return "", nil, err
	}

	rollupDims := m.Dimensions
	rollupDimSet := make(map[string]bool, len(rollupDims))
	for _, d := range rollupDims {
		if !insightsIdentRe.MatchString(d) {
			return "", nil, fmt.Errorf("fabriq: invalid rollup dimension name %q", d)
		}
		rollupDimSet[d] = true
	}
	for _, d := range dims {
		if !rollupDimSet[d] {
			return "", nil, fmt.Errorf("fabriq: requested dimension %q is not a rollup dimension of metric %q", d, m.Name)
		}
	}

	partials, err := planRollupMeasures(measures)
	if err != nil {
		return "", nil, err
	}

	args = []any{tid} // $1 — tenant_id, shared by both CTEs
	argN := 2

	// ---- grain-aligned sealed/live boundary (see this file's header
	// comment for the derivation) ----
	//
	// sealedLo/sealedHiExcl are the ALIGNED tiling boundary: sealed serves
	// bucket_start ∈ [sealedLo, sealedHiExcl); live serves everything else in
	// [q.From, q.To). Both are zero (Go zero time.Time) when there is no
	// corresponding bound (q.From unset -> sealedLo unset: no leading partial
	// to worry about; q.To unset -> sealedHiExcl capped only by watermark).
	g := m.Rollup.Bucket
	var sealedLo, sealedHiExcl time.Time
	if hasWatermark {
		if !q.From.IsZero() {
			sealedLo = ceilToGrain(q.From, g)
		}
		sealedHiExcl = watermark
		if !q.To.IsZero() {
			if toFloor := floorToGrain(q.To, g); toFloor.Before(sealedHiExcl) {
				sealedHiExcl = toFloor
			}
		}
	}

	// ---- sealed CTE: bucket_start ∈ [sealedLo, sealedHiExcl) ----
	sealedSelect := []string{"bucket_start", "scope_id"}
	for _, d := range rollupDims {
		sealedSelect = append(sealedSelect, fmt.Sprintf("%q", d))
	}
	for _, p := range partials {
		for _, c := range p.cols {
			sealedSelect = append(sealedSelect, fmt.Sprintf("%q", c))
		}
	}

	var sealedWhere strings.Builder
	sealedWhere.WriteString("tenant_id = $1")
	if !hasWatermark {
		// No maintainer pass has ever sealed anything for this metric: the
		// sealed partial is unconditionally empty, and the live CTE below
		// (whose lower bound falls back to q.From in this case) covers the
		// entire requested range — still exactly correct, just unaccelerated.
		sealedWhere.WriteString(" AND FALSE")
	} else {
		if !sealedLo.IsZero() {
			fmt.Fprintf(&sealedWhere, " AND bucket_start >= $%d", argN)
			args = append(args, sealedLo)
			argN++
		}
		fmt.Fprintf(&sealedWhere, " AND bucket_start < $%d", argN)
		args = append(args, sealedHiExcl)
		argN++
		for _, c := range q.Filter {
			frag, fargs, ferr := mapCondToRollupDim(c, &argN, rollupDimSet)
			if ferr != nil {
				return "", nil, ferr
			}
			sealedWhere.WriteString(" AND ")
			sealedWhere.WriteString(frag)
			args = append(args, fargs...)
		}
	}
	// If sealedHiExcl <= sealedLo (e.g. the whole query range sits strictly
	// inside a single not-yet-sealed span, or before any watermark has ever
	// advanced past it), the two bound comparisons above naturally yield zero
	// sealed rows — no special-case branch needed; the live CTE's
	// OR-exclusion below (built from the SAME sealedLo/sealedHiExcl values)
	// then covers the entire [q.From, q.To) range, the "all-live" case.
	sealedSQL := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(sealedSelect, ", "), table, sealedWhere.String())

	// ---- live CTE grain arg (bounds computed below, once sealedLo/
	// sealedHiExcl are in scope — see the accurate comment at their WHERE
	// clause) ----
	grainArg := fmt.Sprintf("$%d", argN)
	args = append(args, fmt.Sprintf("%d seconds", int64(m.Rollup.Bucket/time.Second)))
	argN++

	liveSelect := []string{fmt.Sprintf("time_bucket(%s::interval, at) AS bucket_start", grainArg), "scope_id"}
	liveGroups := []string{"bucket_start", "scope_id"}
	for _, d := range rollupDims {
		acc, aerr := propAccessor("props", d)
		if aerr != nil {
			return "", nil, aerr
		}
		liveSelect = append(liveSelect, fmt.Sprintf("%s AS %q", acc, d))
		liveGroups = append(liveGroups, fmt.Sprintf("%q", d))
	}
	for _, p := range partials {
		switch p.kind {
		case query.MeasureCount:
			liveSelect = append(liveSelect, fmt.Sprintf("count(*)::numeric AS %q", p.cols[0]))
		case query.MeasureSum:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			liveSelect = append(liveSelect, fmt.Sprintf("sum((%s)::numeric) AS %q", acc, p.cols[0]))
		case query.MeasureMin:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			liveSelect = append(liveSelect, fmt.Sprintf("min((%s)::numeric) AS %q", acc, p.cols[0]))
		case query.MeasureMax:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			liveSelect = append(liveSelect, fmt.Sprintf("max((%s)::numeric) AS %q", acc, p.cols[0]))
		case query.MeasureAvg:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			liveSelect = append(liveSelect,
				fmt.Sprintf("sum((%s)::numeric) AS %q", acc, p.cols[0]),
				fmt.Sprintf("count(%s)::numeric AS %q", acc, p.cols[1]),
			)
		case query.MeasureCountDistinct:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			// SAME size (rollupHLLRegisters) the maintainer's sealed
			// hyperloglog(...) uses (insights_rollup_maintain.go) — required
			// for the final SELECT's rollup(hll) to combine sealed + live-tail
			// correctly.
			liveSelect = append(liveSelect, fmt.Sprintf("hyperloglog(%d, %s) AS %q", rollupHLLRegisters, acc, p.cols[0]))
		case query.MeasurePercentile:
			acc, aerr := propAccessor("props", p.field)
			if aerr != nil {
				return "", nil, aerr
			}
			// SAME size (rollupTDigestBuckets) the maintainer's sealed
			// tdigest(...) uses — required for rollup(td) to combine
			// correctly, mirroring the hyperloglog case above.
			liveSelect = append(liveSelect, fmt.Sprintf("tdigest(%d, (%s)::double precision) AS %q", rollupTDigestBuckets, acc, p.cols[0]))
		}
	}

	var liveWhere strings.Builder
	fmt.Fprintf(&liveWhere, "tenant_id = $1 AND name = $%d", argN)
	args = append(args, m.Source)
	argN++

	// ---- live CTE bounds: at ∈ [q.From, q.To), EXCLUDING whatever the
	// sealed CTE above already served ([sealedLo, sealedHiExcl)) ----
	if !q.From.IsZero() {
		fmt.Fprintf(&liveWhere, " AND at >= $%d", argN)
		args = append(args, q.From)
		argN++
	}
	if !q.To.IsZero() {
		fmt.Fprintf(&liveWhere, " AND at < $%d", argN)
		args = append(args, q.To)
		argN++
	}
	if hasWatermark {
		// Excludes the sealed interior so sealed ∪ live tiles [q.From, q.To)
		// with no overlap: "at < sealedLo" covers the leading partial bucket
		// (only meaningful — i.e. only added — when q.From carved one out);
		// "at >= sealedHiExcl" covers the trailing partial bucket plus
		// whatever unsealed tail remains, and is always added when a
		// watermark exists. If sealedHiExcl <= sealedLo (sealed was empty),
		// this OR is a tautology (every `at` satisfies one side or the
		// other), so live correctly serves the entire range unfiltered by
		// this clause — the "all-live" case.
		var orParts []string
		if !sealedLo.IsZero() {
			orParts = append(orParts, fmt.Sprintf("at < $%d", argN))
			args = append(args, sealedLo)
			argN++
		}
		orParts = append(orParts, fmt.Sprintf("at >= $%d", argN))
		args = append(args, sealedHiExcl)
		argN++
		fmt.Fprintf(&liveWhere, " AND (%s)", strings.Join(orParts, " OR "))
	}
	for _, c := range q.Filter {
		frag, fargs, ferr := mapCondToProp(c, &argN, "props", rollupDimSet)
		if ferr != nil {
			return "", nil, ferr
		}
		fmt.Fprintf(&liveWhere, " AND %s", frag)
		args = append(args, fargs...)
	}

	liveSQL := fmt.Sprintf("SELECT %s FROM fabriq_insights_events WHERE %s GROUP BY %s",
		strings.Join(liveSelect, ", "), liveWhere.String(), strings.Join(liveGroups, ", "))

	// ---- final: re-bucket + re-aggregate the combined parts ----
	allowedOrder := map[string]bool{"bucket": true}
	aliasExpr := map[string]string{}

	finalSelect := []string{fmt.Sprintf("time_bucket($%d::interval, bucket_start) AS bucket", argN)}
	args = append(args, fmt.Sprintf("%d seconds", int64(bucket/time.Second)))
	argN++
	finalGroups := []string{"bucket"}

	for _, d := range dims {
		finalSelect = append(finalSelect, fmt.Sprintf("%q AS %q", d, d))
		finalGroups = append(finalGroups, fmt.Sprintf("%q", d))
		allowedOrder[d] = true
	}

	for _, p := range partials {
		var expr string
		switch p.kind {
		case query.MeasureCount:
			expr = fmt.Sprintf("sum(%q)", p.cols[0])
		case query.MeasureSum:
			expr = fmt.Sprintf("sum(%q)", p.cols[0])
		case query.MeasureMin:
			expr = fmt.Sprintf("min(%q)", p.cols[0])
		case query.MeasureMax:
			expr = fmt.Sprintf("max(%q)", p.cols[0])
		case query.MeasureAvg:
			expr = fmt.Sprintf("(sum(%q) / NULLIF(sum(%q), 0))", p.cols[0], p.cols[1])
		case query.MeasureCountDistinct:
			// rollup(hll) unions every sealed-bucket hll AND the live-tail
			// hll (both built at rollupHLLRegisters, so they combine
			// validly) within this GROUP; distinct_count reads the
			// estimate off the combined hll. Approximate by construction —
			// the design's explicit trade for a Rollup metric with a
			// count_distinct measure.
			expr = fmt.Sprintf("distinct_count(rollup(%q))", p.cols[0])
		case query.MeasurePercentile:
			// Mirrors the count_distinct case above for tdigest: rollup(td)
			// combines every sealed + live-tail partial (all built at
			// rollupTDigestBuckets) within this GROUP; approx_percentile
			// reads the requested fraction off the combined digest. The
			// fraction travels as a bound $N parameter (like the live
			// path's percentile_cont — measureAggExpr,
			// insights_query_build.go) rather than a literal, consistent
			// with this file's own "every value is a bound parameter"
			// injection-guard contract.
			ph := fmt.Sprintf("$%d", argN)
			args = append(args, p.percentile)
			argN++
			expr = fmt.Sprintf("approx_percentile(%s, rollup(%q))", ph, p.cols[0])
		}
		finalSelect = append(finalSelect, fmt.Sprintf("%s AS %q", expr, p.alias))
		allowedOrder[p.alias] = true
		aliasExpr[p.alias] = expr
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "WITH sealed AS (%s), live AS (%s), parts AS (SELECT * FROM sealed UNION ALL SELECT * FROM live) SELECT %s FROM parts",
		sealedSQL, liveSQL, strings.Join(finalSelect, ", "))
	fmt.Fprintf(&sb, " GROUP BY %s", strings.Join(finalGroups, ", "))

	if len(q.Having) > 0 {
		var havingParts []string
		for _, c := range q.Having {
			frag, hargs, herr := mapHavingCond(c, aliasExpr, &argN)
			if herr != nil {
				return "", nil, herr
			}
			havingParts = append(havingParts, frag)
			args = append(args, hargs...)
		}
		fmt.Fprintf(&sb, " HAVING %s", strings.Join(havingParts, " AND "))
	}

	if q.OrderBy != "" {
		ord, oerr := buildInsightsOrder(q.OrderBy, allowedOrder)
		if oerr != nil {
			return "", nil, oerr
		}
		fmt.Fprintf(&sb, " ORDER BY %s", ord)
	} else {
		fmt.Fprintf(&sb, " ORDER BY %s", strings.Join(finalGroups, ", "))
	}
	if q.Limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %d", q.Limit)
	}
	if q.Offset > 0 {
		fmt.Fprintf(&sb, " OFFSET %d", q.Offset)
	}

	return sb.String(), args, nil
}
