package fabriqtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// FakeAnalytics is an in-memory query.AnalyticsQuerier for tests. Events are
// keyed tenant-first so cross-tenant reads are structurally impossible.
type FakeAnalytics struct {
	mu    sync.Mutex
	reg   *registry.Registry
	data  map[string][]query.AnalyticsEvent // tenant -> events
	seen  map[string]bool                   // tenant|dedupKey -> true
	facts map[string]insights.Fact          // tenant|entity|aggID -> latest surviving fact
}

// NewFakeAnalytics returns an empty FakeAnalytics that resolves Query.Source
// against reg via insights.ResolveSource — the same resolver the real
// Postgres adapter uses, so routing (metric > entity > event) can't drift
// between the fake and the adapter. reg may be nil, in which case every
// source resolves to an event descriptor (the prior back-compat behavior).
func NewFakeAnalytics(reg *registry.Registry) *FakeAnalytics {
	return &FakeAnalytics{
		reg:   reg,
		data:  map[string][]query.AnalyticsEvent{},
		seen:  map[string]bool{},
		facts: map[string]insights.Fact{},
	}
}

// var _ insights.FactSink asserts FakeAnalytics also implements the
// proj:insights consumer's write port, alongside query.AnalyticsQuerier
// below — mirroring *postgres.InsightsAdapter (adapters/postgres/insights.go),
// which implements both on one receiver too.
var _ insights.FactSink = (*FakeAnalytics)(nil)

// UpsertInsightFacts implements insights.FactSink: an in-memory, version-
// gated upsert keyed tenant|entity|aggID, mirroring the real adapter's
// (tenant_id, entity, agg_id) uniqueness + "WHERE EXCLUDED.version >
// fabriq_insights_facts.version" gate (adapters/postgres/insights.go's
// UpsertInsightFacts) — an equal-or-older version is a silent no-op, never
// an error, so redelivery from the proj:insights consumer stays idempotent.
func (f *FakeAnalytics) UpsertInsightFacts(ctx context.Context, facts []insights.Fact) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, fact := range facts {
		k := tid + "|" + fact.Entity + "|" + fact.AggID
		if existing, ok := f.facts[k]; ok && existing.Version >= fact.Version {
			continue // version gate: an equal-or-older write never replaces the stored fact
		}
		f.facts[k] = fact
	}
	return nil
}

// Track implements query.AnalyticsQuerier. Events sharing a non-empty
// DedupKey (scoped per tenant) after the first Track call are dropped.
func (f *FakeAnalytics) Track(ctx context.Context, events []query.AnalyticsEvent) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range events {
		if e.DedupKey != "" {
			k := tid + "|" + e.DedupKey
			if f.seen[k] {
				continue
			}
			f.seen[k] = true
		}
		f.data[tid] = append(f.data[tid], e)
	}
	return nil
}

// QueryRaw implements query.AnalyticsQuerier. Raw SQL has no in-memory
// analogue; the fake always errors. The conformance suite does not exercise
// this method — only the real adapter does.
func (f *FakeAnalytics) QueryRaw(ctx context.Context, into any, sql string, args ...any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	return fmt.Errorf("fabriqtest: FakeAnalytics does not implement QueryRaw")
}

// Query implements query.AnalyticsQuerier by aggregating in-memory: filter by
// Source/time-window/Filter, group by (Dimensions..., time bucket), fold
// Measures, order deterministically, and apply Limit. Output rows are
// marshaled into `into` (a *[]map[string]any) via a JSON round-trip.
func (f *FakeAnalytics) Query(ctx context.Context, q query.AnalyticsQuery, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	d, err := insights.ResolveSource(f.reg, q.Source)
	if err != nil {
		return err
	}
	measures, dimensions, bucket, err := insights.EffectiveQuery(q, d)
	if err != nil {
		return err
	}
	if verr := checkInsightsColumns(d, measures, dimensions, q.Filter); verr != nil {
		return verr
	}
	if verr := checkPercentileFractions(measures); verr != nil {
		return verr
	}

	var rows []query.AnalyticsEvent
	if d.Kind == insights.SourceFacts {
		rows, err = f.factsAsEvents(tid, d)
		if err != nil {
			return err
		}
	} else {
		f.mu.Lock()
		rows = append([]query.AnalyticsEvent(nil), f.data[tid]...)
		f.mu.Unlock()
	}

	// 1. filter by Source (event name), time window, and Filter predicates.
	var filtered []query.AnalyticsEvent
	for _, e := range rows {
		if d.KeyValue != "" && e.Name != d.KeyValue {
			continue
		}
		if !q.From.IsZero() && e.At.Before(q.From) {
			continue
		}
		if !q.To.IsZero() && !e.At.Before(q.To) {
			continue
		}
		ok, err := matchWhere(e.Props, q.Filter)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		filtered = append(filtered, e)
	}

	// 2. group by (dimensions..., bucket) and fold measures.
	type groups struct {
		order []string
		byKey map[string][]query.AnalyticsEvent
	}
	g := groups{byKey: map[string][]query.AnalyticsEvent{}}
	for _, e := range filtered {
		k := groupKey(e, dimensions, bucket)
		if _, ok := g.byKey[k]; !ok {
			g.order = append(g.order, k)
		}
		g.byKey[k] = append(g.byKey[k], e)
	}

	out := make([]map[string]any, 0, len(g.order))
	for _, k := range g.order {
		rowsInGroup := g.byKey[k]
		row := map[string]any{}
		for _, dm := range dimensions {
			row[dm] = rowsInGroup[0].Props[dm]
		}
		if bucket > 0 {
			row["bucket"] = bucketOf(rowsInGroup[0].At, bucket)
		}
		for _, m := range measures {
			row[measureName(m)] = foldMeasure(m, rowsInGroup)
		}
		out = append(out, row)
	}

	// 2.5. post-aggregation filter (Having): the row map's keys ARE the
	// measure aliases (measureName), so evalConds — the same in-memory
	// predicate evaluator used for pre-aggregation Filter above — applies
	// unchanged; numeric measure values are float64/int64, which
	// evalConds/toFloat already coerce.
	if len(q.Having) > 0 {
		if verr := checkHavingAliases(q.Having, measures); verr != nil {
			return verr
		}
		kept := make([]map[string]any, 0, len(out))
		for _, row := range out {
			ok, err := evalConds(row, q.Having)
			if err != nil {
				return err
			}
			if ok {
				kept = append(kept, row)
			}
		}
		out = kept
	}

	// 3. deterministic order (dimensions then bucket) unless the caller wants
	// only a bounded slice via Limit.
	sort.SliceStable(out, func(i, j int) bool { return lessRows(out[i], out[j], dimensions) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return assignJSON(into, out)
}

var _ query.AnalyticsQuerier = (*FakeAnalytics)(nil)

// factsAsEvents reads the tenant's surviving (non-deleted) facts for the
// projected entity d.KeyValue and converts each into a synthetic
// query.AnalyticsEvent — Name: d.KeyValue (so the Source-name filter in Query
// is a no-op), Props: the fact's JSON payload unmarshaled to a map, At: the
// fact's own timestamp (for time-bucketing). This lets Query's existing
// filter/group/measure/having/order/limit pipeline aggregate facts without
// any separate code path — only the row source differs from the event case.
func (f *FakeAnalytics) factsAsEvents(tid string, d insights.Descriptor) ([]query.AnalyticsEvent, error) {
	f.mu.Lock()
	matched := make([]insights.Fact, 0, len(f.facts))
	for _, fact := range f.facts {
		if fact.TenantID != tid || fact.Entity != d.KeyValue || fact.Deleted {
			continue
		}
		matched = append(matched, fact)
	}
	f.mu.Unlock()

	out := make([]query.AnalyticsEvent, 0, len(matched))
	for _, fact := range matched {
		var props map[string]any
		if len(fact.Payload) > 0 {
			if err := json.Unmarshal(fact.Payload, &props); err != nil {
				return nil, fmt.Errorf("fabriq: FakeAnalytics: unmarshal fact payload for entity %q agg_id %q: %w", fact.Entity, fact.AggID, err)
			}
		}
		out = append(out, query.AnalyticsEvent{Name: d.KeyValue, At: fact.At, Props: props})
	}
	return out, nil
}

// checkInsightsColumns rejects a Dimension, Measure field, or Filter column
// that isn't declared in d.AllowedColumns — the InsightsSpec column allow-
// list resolved for a SourceFacts query (nil for SourceEvent, where any
// top-level prop key is allowed, so this is a no-op there). Mirrors the
// real adapter's identical checks in measureAggExpr/mapCondToProp/the
// dimension loop of buildInsightsSQL (adapters/postgres/insights_query_build.go),
// including the exact error wording, so fake and adapter agree on both
// WHETHER a query is rejected and WHAT the error says. measures/dimensions
// are the EFFECTIVE ones (insights.EffectiveQuery output) so a metric's own
// declared measures/dimensions are checked too, not just what the caller
// passed in q; where is always the caller's (query.AnalyticsQuery.Filter is
// never metric-expanded).
func checkInsightsColumns(d insights.Descriptor, measures []query.Measure, dimensions []string, where query.Where) error {
	if d.AllowedColumns == nil {
		return nil
	}
	for _, dim := range dimensions {
		if !d.AllowedColumns[dim] {
			return fmt.Errorf("fabriq: insights dimension %q is not declared for this source", dim)
		}
	}
	for _, m := range measures {
		if m.Kind == query.MeasureCount {
			continue // COUNT(*) has no Field to check
		}
		if !d.AllowedColumns[m.Field] {
			return fmt.Errorf("fabriq: insights column %q is not declared for this source", m.Field)
		}
	}
	return checkAllowedInWhere(where, d.AllowedColumns)
}

// checkAllowedInWhere recursively checks every leaf condition's Column
// (descending into OR groups) against allowed, mirroring mapCondToProp's
// per-condition check in the real adapter.
func checkAllowedInWhere(where query.Where, allowed map[string]bool) error {
	for _, c := range where {
		if c.IsGroup() {
			if err := checkAllowedInWhere(c.Or, allowed); err != nil {
				return err
			}
			continue
		}
		if !allowed[c.Column] {
			return fmt.Errorf("fabriq: insights column %q is not declared for this source", c.Column)
		}
	}
	return nil
}

// checkPercentileFractions rejects any MeasurePercentile whose Percentile
// fraction is not in the open interval (0,1), mirroring the real adapter's
// identical guard in measureAggExpr (adapters/postgres/insights_query_build.go),
// including its exact error wording. Without this upfront check, foldMeasure's
// MeasurePercentile case computes h=Percentile*(n-1) and indexes vals[floor(h)]/
// vals[ceil(h)] unguarded — a fraction outside (0,1) (e.g. 1.5, or a negative
// value) produces an out-of-range index and PANICS instead of erroring, which
// is exactly the fake/adapter error-path divergence this guard closes.
func checkPercentileFractions(measures []query.Measure) error {
	for _, m := range measures {
		if m.Kind == query.MeasurePercentile && !(m.Percentile > 0 && m.Percentile < 1) {
			return fmt.Errorf("fabriq: percentile must be in (0,1), got %v", m.Percentile)
		}
	}
	return nil
}

// checkHavingAliases rejects any Having condition (recursing into OR groups)
// whose Column is not one of the query's own emitted measure output-aliases
// (measureName), mirroring the real adapter's mapHavingCond, which looks
// c.Column up in its alias->aggExpr map and errors with the same wording on a
// miss — including for a bare dimension name, which is never a valid HAVING
// column in either implementation (HAVING filters post-aggregation measure
// values, not group-by keys). Without this check the fake's Having path
// (evalConds against the output-row map) treats an unknown alias as "column
// absent from row" and evaluates every condition false, silently dropping
// every row, where the adapter errors — the other fake/adapter divergence
// this file closes.
func checkHavingAliases(having query.Where, measures []query.Measure) error {
	aliases := make(map[string]bool, len(measures))
	for _, m := range measures {
		aliases[measureName(m)] = true
	}
	var check func(query.Where) error
	check = func(w query.Where) error {
		for _, c := range w {
			if c.IsGroup() {
				if err := check(c.Or); err != nil {
					return err
				}
				continue
			}
			if !aliases[c.Column] {
				return fmt.Errorf("fabriq: having references unknown measure alias %q", c.Column)
			}
		}
		return nil
	}
	return check(having)
}

// assignJSON marshals src to JSON and unmarshals it into dst (typically a
// pointer to a slice). fabriqtest has no shared generic scan helper yet — the
// existing fakes each type-assert `into` to a concrete type instead — so this
// is a small local round-trip used only by FakeAnalytics.Query.
func assignJSON(dst any, src any) error {
	buf, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("fabriq: FakeAnalytics: marshal result: %w", err)
	}
	if err := json.Unmarshal(buf, dst); err != nil {
		return fmt.Errorf("fabriq: FakeAnalytics: unmarshal into %T: %w", dst, err)
	}
	return nil
}

// matchWhere reports whether props satisfies every condition in where (AND).
// It reuses fabriqtest's existing evalConds (filter.go) — the same in-memory
// predicate evaluator FakeRelational.List uses — so Eq/Ne/In/NotIn/Gt/Gte/
// Lt/Lte/Like/ILike/IsNull/IsNotNull and OR groups all behave identically
// across the fake ports. The brief only requires Eq/In over top-level props;
// reuse gets the rest for free.
func matchWhere(props map[string]any, where query.Where) (bool, error) {
	return evalConds(props, where)
}

// groupKey builds a stable string key for a group of events sharing the same
// dimension values and time bucket.
func groupKey(e query.AnalyticsEvent, dims []string, bucket time.Duration) string {
	key := ""
	for _, d := range dims {
		key += d + "=" + fmt.Sprintf("%v", e.Props[d]) + "\x1f"
	}
	if bucket > 0 {
		key += "bucket=" + bucketOf(e.At, bucket).Format(time.RFC3339Nano)
	}
	return key
}

// bucketOf truncates at to the start of its bucket, in UTC.
func bucketOf(at time.Time, bucket time.Duration) time.Time {
	return at.Truncate(bucket).UTC()
}

// measureName is the output column name for a measure: As when set, else
// "count" for MeasureCount, else "p<pct>_<field>" for MeasurePercentile
// (MUST match measureAlias's default in adapters/postgres/insights_query_build.go
// exactly, so the fake and the real adapter agree on column names for the
// same Measure), else "<kind>_<field>".
func measureName(m query.Measure) string {
	if m.As != "" {
		return m.As
	}
	if m.Kind == query.MeasureCount {
		return string(m.Kind)
	}
	if m.Kind == query.MeasurePercentile {
		return fmt.Sprintf("p%d_%s", int(math.Round(m.Percentile*100)), m.Field)
	}
	return string(m.Kind) + "_" + m.Field
}

// foldMeasure aggregates one measure over a group of events.
func foldMeasure(m query.Measure, rows []query.AnalyticsEvent) any {
	switch m.Kind {
	case query.MeasureCount:
		return int64(len(rows))
	case query.MeasureCountDistinct:
		seen := map[string]bool{}
		for _, e := range rows {
			seen[fmt.Sprintf("%v", e.Props[m.Field])] = true
		}
		return int64(len(seen))
	case query.MeasureSum:
		var sum float64
		for _, e := range rows {
			if v, ok := toFloat(e.Props[m.Field]); ok {
				sum += v
			}
		}
		return sum
	case query.MeasureAvg:
		var sum float64
		var n int
		for _, e := range rows {
			if v, ok := toFloat(e.Props[m.Field]); ok {
				sum += v
				n++
			}
		}
		if n == 0 {
			return nil
		}
		return sum / float64(n)
	case query.MeasureMin:
		var min float64
		var set bool
		for _, e := range rows {
			if v, ok := toFloat(e.Props[m.Field]); ok {
				if !set || v < min {
					min = v
					set = true
				}
			}
		}
		if !set {
			return nil
		}
		return min
	case query.MeasureMax:
		var max float64
		var set bool
		for _, e := range rows {
			if v, ok := toFloat(e.Props[m.Field]); ok {
				if !set || v > max {
					max = v
					set = true
				}
			}
		}
		if !set {
			return nil
		}
		return max
	case query.MeasurePercentile:
		var vals []float64
		for _, e := range rows {
			if v, ok := toFloat(e.Props[m.Field]); ok {
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return nil
		}
		sort.Float64s(vals)
		n := len(vals)
		h := m.Percentile * float64(n-1)
		lo := int(math.Floor(h))
		hi := int(math.Ceil(h))
		if lo == hi {
			return vals[lo]
		}
		return vals[lo] + (h-float64(lo))*(vals[hi]-vals[lo])
	default:
		return nil
	}
}

// lessRows orders two output rows by dimension values (in Dimensions order),
// then by the "bucket" column when present. It gives the conformance suite
// (and callers) a deterministic default ordering when AnalyticsQuery.OrderBy
// is empty.
func lessRows(a, b map[string]any, dims []string) bool {
	for _, d := range dims {
		av, bv := fmt.Sprintf("%v", a[d]), fmt.Sprintf("%v", b[d])
		if av != bv {
			return av < bv
		}
	}
	ab, aok := a["bucket"].(time.Time)
	bb, bok := b["bucket"].(time.Time)
	if aok && bok {
		return ab.Before(bb)
	}
	return false
}

// toFloat is reused from filter.go (fabriqtest's existing numeric coercion
// used by evalConds/compareVals). It handles the int/uint/float family that
// query.AnalyticsEvent.Props natively carries (events are tracked from Go
// literals, not decoded JSON, so json.Number never appears here).
