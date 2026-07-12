package fabriqtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// FakeAnalytics is an in-memory query.AnalyticsQuerier for tests. Events are
// keyed tenant-first so cross-tenant reads are structurally impossible.
type FakeAnalytics struct {
	mu   sync.Mutex
	data map[string][]query.AnalyticsEvent // tenant -> events
	seen map[string]bool                   // tenant|dedupKey -> true
}

// NewFakeAnalytics returns an empty FakeAnalytics.
func NewFakeAnalytics() *FakeAnalytics {
	return &FakeAnalytics{data: map[string][]query.AnalyticsEvent{}, seen: map[string]bool{}}
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
	f.mu.Lock()
	rows := append([]query.AnalyticsEvent(nil), f.data[tid]...)
	f.mu.Unlock()

	// 1. filter by Source (event name), time window, and Filter predicates.
	var filtered []query.AnalyticsEvent
	for _, e := range rows {
		if q.Source != "" && e.Name != q.Source {
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
		k := groupKey(e, q.Dimensions, q.TimeBucket)
		if _, ok := g.byKey[k]; !ok {
			g.order = append(g.order, k)
		}
		g.byKey[k] = append(g.byKey[k], e)
	}

	out := make([]map[string]any, 0, len(g.order))
	for _, k := range g.order {
		rowsInGroup := g.byKey[k]
		row := map[string]any{}
		for _, d := range q.Dimensions {
			row[d] = rowsInGroup[0].Props[d]
		}
		if q.TimeBucket > 0 {
			row["bucket"] = bucketOf(rowsInGroup[0].At, q.TimeBucket)
		}
		for _, m := range q.Measures {
			row[measureName(m)] = foldMeasure(m, rowsInGroup)
		}
		out = append(out, row)
	}

	// 3. deterministic order (dimensions then bucket) unless the caller wants
	// only a bounded slice via Limit.
	sort.SliceStable(out, func(i, j int) bool { return lessRows(out[i], out[j], q.Dimensions) })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return assignJSON(into, out)
}

var _ query.AnalyticsQuerier = (*FakeAnalytics)(nil)

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
// "count" for MeasureCount, else "<kind>_<field>".
func measureName(m query.Measure) string {
	if m.As != "" {
		return m.As
	}
	if m.Kind == query.MeasureCount {
		return string(m.Kind)
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
