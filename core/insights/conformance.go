// Package insights holds the behavioral contract for the per-tenant,
// customer-facing analytics port (query.AnalyticsQuerier). It is deliberately
// distinct from core/analytics, which is the operator-facing cross-tenant
// sink conformance suite for a different port (analytics.Sink).
package insights

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// conformanceOrderModel is a grove-tagged fixture used ONLY to register the
// suite's "order" InsightsSpec entity below (Register requires a Model or a
// Schema). Its own table is never created or queried: SourceFacts always
// reads fabriq_insights_facts (a resolver-produced constant — see
// insights.Descriptor.Table), never an entity's own bound table, so this
// model needs no matching migration. A distinct table name
// ("insights_conformance_orders") avoids any confusion with the
// superficially similar fixtures in resolve_test.go (resolveOrderModel,
// table "resolve_orders") and core/registry/metric_index_test.go
// (table "metric_orders") — different packages, but kept unique for clarity.
type conformanceOrderModel struct {
	grove.BaseModel `grove:"table:insights_conformance_orders"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Amount   int64  `grove:"amount"`
	Status   string `grove:"status"`
}

// RunConformance is the single behavioral contract every query.AnalyticsQuerier
// must satisfy. fabriqtest runs it against FakeAnalytics; adapters/postgres runs
// it against real Postgres. Drift is a failing test.
//
// It exercises Track + Query only. QueryRaw has no portable in-memory
// contract (raw SQL is dialect-specific): the adapter's own test suite
// exercises QueryRaw separately.
//
// newQ is invoked once per sub-test (some fakes truncate state per-call) and
// is handed the suite's registry so it can wire the querier under test
// (fake or real adapter) with the same reg every time — the registry the
// insights.ResolveSource routing decisions are made against. The suite's own
// existing event-only subtests need nothing registered in reg; Tasks 6/7 add
// entities/metrics to it for the facts/metric subtests they introduce.
func RunConformance(t *testing.T, newQ func(reg *registry.Registry) query.AnalyticsQuerier) {
	reg := registry.New()
	if err := reg.Validate(); err != nil {
		t.Fatalf("conformance suite registry: %v", err)
	}
	ctx1 := mustTenant(t, "t1")
	ctx2 := mustTenant(t, "t2")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("CountBySingleDimension", func(t *testing.T) {
		q := newQ(reg)
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
		paid := findByStatus(rows, "paid")
		if paid == nil || asInt(paid["n"]) != 2 || asInt(paid["total"]) != 15 {
			t.Fatalf("paid group wrong: %+v", paid)
		}
		void := findByStatus(rows, "void")
		if void == nil || asInt(void["n"]) != 1 || asInt(void["total"]) != 0 {
			t.Fatalf("void group wrong: %+v", void)
		}
		if len(rows) != 2 {
			t.Fatalf("want 2 groups, got %d: %+v", len(rows), rows)
		}
	})

	t.Run("TimeBucketGroups", func(t *testing.T) {
		q := newQ(reg)
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
		q := newQ(reg)
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
		q := newQ(reg)
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
		q := newQ(reg)
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
		q := newQ(reg)
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
		q := newQ(reg)
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
		q := newQ(reg)
		ev := query.AnalyticsEvent{Name: "order", At: base, Props: map[string]any{}, DedupKey: "k1"}
		must(t, q.Track(ctx1, []query.AnalyticsEvent{ev}))
		must(t, q.Track(ctx1, []query.AnalyticsEvent{ev})) // replay
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{Source: "order", Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}}}, &rows))
		if len(rows) != 1 || asInt(rows[0]["n"]) != 1 {
			t.Fatalf("dedup failed: %+v", rows)
		}
	})

	t.Run("FilterGtNumeric", func(t *testing.T) {
		q := newQ(reg)
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"amount": 50}},
			{Name: "order", At: base, Props: map[string]any{"amount": 100}},
			{Name: "order", At: base, Props: map[string]any{"amount": 150}},
			{Name: "order", At: base, Props: map[string]any{"amount": 200}},
		}))
		var gt []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Gt("amount", 100)},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &gt))
		// Strictly greater than 100 straddles the boundary: 150 and 200 match,
		// 50 and 100 (the boundary value itself) do not.
		if len(gt) != 1 || asInt(gt[0]["n"]) != 2 {
			t.Fatalf("gt filter wrong: %+v", gt)
		}

		var gte []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Gte("amount", 100)},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &gte))
		if len(gte) != 1 || asInt(gte[0]["n"]) != 3 {
			t.Fatalf("gte filter wrong: %+v", gte)
		}

		var lt []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Lt("amount", 100)},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &lt))
		if len(lt) != 1 || asInt(lt[0]["n"]) != 1 {
			t.Fatalf("lt filter wrong: %+v", lt)
		}

		var lte []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Lte("amount", 100)},
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &lte))
		if len(lte) != 1 || asInt(lte[0]["n"]) != 2 {
			t.Fatalf("lte filter wrong: %+v", lte)
		}
	})

	t.Run("LimitBoundsRows", func(t *testing.T) {
		q := newQ(reg)
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

	t.Run("Percentile", func(t *testing.T) {
		q := newQ(reg)
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "latency", At: base, Props: map[string]any{"ms": 10}},
			{Name: "latency", At: base, Props: map[string]any{"ms": 20}},
			{Name: "latency", At: base, Props: map[string]any{"ms": 30}},
			{Name: "latency", At: base, Props: map[string]any{"ms": 40}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:   "latency",
			Measures: []query.Measure{{Kind: query.MeasurePercentile, Field: "ms", Percentile: 0.5, As: "p50"}},
		}, &rows))
		if len(rows) != 1 {
			t.Fatalf("want 1 grand-total row, got %d: %+v", len(rows), rows)
		}
		// Median via continuous linear interpolation: n=4, p=0.5, h=p*(n-1)=1.5,
		// lo=1 (v=20), hi=2 (v=30) -> 20 + 0.5*(30-20) = 25. Matches Postgres
		// percentile_cont(0.5) exactly, so this locks fake/real parity.
		if p50, ok := toFloatT(rows[0]["p50"]); !ok || p50 != 25 {
			t.Fatalf("percentile p50 wrong: %+v", rows[0])
		}
	})

	t.Run("RejectsBadPercentile", func(t *testing.T) {
		q := newQ(reg)
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "latency3", At: base, Props: map[string]any{"ms": 10}},
		}))
		var rows []map[string]any
		err := q.Query(ctx1, query.AnalyticsQuery{
			Source:   "latency3",
			Measures: []query.Measure{{Kind: query.MeasurePercentile, Field: "ms", Percentile: 1.5}},
		}, &rows)
		if err == nil {
			t.Fatal("want error: percentile fraction 1.5 is out of (0,1) range")
		}
	})

	t.Run("HavingFiltersGroups", func(t *testing.T) {
		q := newQ(reg)
		events := make([]query.AnalyticsEvent, 0, 8)
		for i := 0; i < 6; i++ {
			events = append(events, query.AnalyticsEvent{Name: "order", At: base, Props: map[string]any{"status": "paid"}})
		}
		for i := 0; i < 2; i++ {
			events = append(events, query.AnalyticsEvent{Name: "order", At: base, Props: map[string]any{"status": "void"}})
		}
		must(t, q.Track(ctx1, events))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
			Having:     query.Where{query.Gt("n", 5)},
		}, &rows))
		if len(rows) != 1 || rows[0]["status"] != "paid" || asInt(rows[0]["n"]) != 6 {
			t.Fatalf("having wrong: %+v", rows)
		}
	})

	t.Run("HavingOverPercentile", func(t *testing.T) {
		q := newQ(reg)
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "latency2", At: base, Props: map[string]any{"status": "a", "ms": 10}},
			{Name: "latency2", At: base, Props: map[string]any{"status": "a", "ms": 20}},
			{Name: "latency2", At: base, Props: map[string]any{"status": "a", "ms": 30}},
			{Name: "latency2", At: base, Props: map[string]any{"status": "a", "ms": 40}},
			{Name: "latency2", At: base, Props: map[string]any{"status": "b", "ms": 100}},
			{Name: "latency2", At: base, Props: map[string]any{"status": "b", "ms": 200}},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "latency2",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasurePercentile, Field: "ms", Percentile: 0.5, As: "med"}},
			Having:     query.Where{query.Gt("med", 50)},
		}, &rows))
		// Group "a" (ms 10,20,30,40) -> p50=25, dropped by Having. Group "b"
		// (ms 100,200) -> p50=150, kept. This is the subtlest Having path: the
		// percentile aggregate expression binds its fraction to an already-used
		// $N placeholder (measureAggExpr), and mapHavingCond must repeat that
		// same expression verbatim in HAVING rather than referencing the
		// SELECT-list alias (Postgres cannot do that) or re-binding the
		// fraction. Locks fake/real parity for that path.
		if len(rows) != 1 {
			t.Fatalf("want 1 group past having, got %d: %+v", len(rows), rows)
		}
		if rows[0]["status"] != "b" {
			t.Fatalf("having-over-percentile wrong group: %+v", rows)
		}
		if med, ok := toFloatT(rows[0]["med"]); !ok || med != 150 {
			t.Fatalf("having-over-percentile med wrong: %+v", rows[0])
		}
	})

	t.Run("RejectsHavingUnknownAlias", func(t *testing.T) {
		q := newQ(reg)
		must(t, q.Track(ctx1, []query.AnalyticsEvent{
			{Name: "order", At: base, Props: map[string]any{"status": "paid"}},
		}))
		var rows []map[string]any
		err := q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
			Having:   query.Where{query.Gt("ghost", 1)},
		}, &rows)
		if err == nil {
			t.Fatal("want error: having references unknown measure alias \"ghost\"")
		}
	})

	t.Run("TenantIsolation", func(t *testing.T) {
		q := newQ(reg)
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
		q := newQ(reg)
		if err := q.Track(context.Background(), []query.AnalyticsEvent{{Name: "x"}}); err == nil {
			t.Fatal("want error tracking without a tenant on ctx")
		}
		var rows []map[string]any
		if err := q.Query(context.Background(), query.AnalyticsQuery{Source: "x"}, &rows); err == nil {
			t.Fatal("want error querying without a tenant on ctx")
		}
	})

	// --- Projected-fact querying (Task 6): Query.Source names an entity ---
	//
	// The "order" InsightsSpec entity is registered into reg HERE, after
	// every subtest above has already run to completion, not at the top of
	// RunConformance. reg is one *registry.Registry shared across this whole
	// function; none of the subtests above call t.Parallel(), so t.Run runs
	// each subtest's body synchronously before returning. Several subtests
	// above (CountBySingleDimension, MinMaxAvg, FilterGtNumeric,
	// HavingFiltersGroups, LimitBoundsRows, ...) track events NAMED "order"
	// and query Source: "order" expecting SourceEvent resolution. Registering
	// an InsightsSpec entity named "order" any earlier would flip
	// insights.ResolveSource's precedence (entity > event) for every one of
	// those, breaking them — so the registration must happen only after they
	// have all already resolved "order" as a bare event name.
	if err := reg.Register(registry.EntitySpec{
		Name:  "order",
		Model: (*conformanceOrderModel)(nil),
		Insights: &registry.InsightsSpec{
			Measures:   []string{"amount"},
			Dimensions: []string{"status"},
		},
		// "revenue" is a declared MetricSpec sourcing "order": Query{Source:
		// "revenue"} expands to Sum(amount) As "rev" grouped by status — the
		// metric-by-name subtests below (Task 7) exercise it.
		Metrics: []registry.MetricSpec{{
			Name:       "revenue",
			Source:     "order",
			Measures:   []registry.MetricMeasure{{Kind: "sum", Field: "amount", As: "rev"}},
			Dimensions: []string{"status"},
		}},
	}); err != nil {
		t.Fatalf("conformance suite: register order facts entity: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("conformance suite: re-validate after registering metric: %v", err)
	}

	t.Run("QueryProjectedFacts", func(t *testing.T) {
		q := newQ(reg)
		sink, ok := q.(FactSink)
		if !ok {
			t.Fatalf("%T does not implement insights.FactSink", q)
		}
		must(t, sink.UpsertInsightFacts(ctx1, []Fact{
			{TenantID: "t1", Entity: "order", AggID: "f1", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":10}`)},
			{TenantID: "t1", Entity: "order", AggID: "f2", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":5}`)},
			{TenantID: "t1", Entity: "order", AggID: "f3", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"void","amount":0}`)},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasureSum, Field: "amount", As: "total"}},
		}, &rows))
		paid := findByStatus(rows, "paid")
		if paid == nil || asInt(paid["total"]) != 15 {
			t.Fatalf("paid facts total wrong: %+v", paid)
		}
		void := findByStatus(rows, "void")
		if void == nil || asInt(void["total"]) != 0 {
			t.Fatalf("void facts total wrong: %+v", void)
		}
		if len(rows) != 2 {
			t.Fatalf("want 2 groups, got %d: %+v", len(rows), rows)
		}
	})

	t.Run("ProjectedFactsVersionGate", func(t *testing.T) {
		q := newQ(reg)
		sink, ok := q.(FactSink)
		if !ok {
			t.Fatalf("%T does not implement insights.FactSink", q)
		}
		must(t, sink.UpsertInsightFacts(ctx1, []Fact{
			{TenantID: "t1", Entity: "order", AggID: "vg1", Version: 2, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":20}`)},
		}))
		// An older version for the same (entity, agg_id) must be a silent
		// no-op: the v2 payload (and only the v2 payload) must survive.
		must(t, sink.UpsertInsightFacts(ctx1, []Fact{
			{TenantID: "t1", Entity: "order", AggID: "vg1", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"void","amount":999}`)},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasureSum, Field: "amount", As: "total"}},
		}, &rows))
		if len(rows) != 1 {
			t.Fatalf("version gate: want exactly 1 group (v2's), got %d: %+v", len(rows), rows)
		}
		if rows[0]["status"] != "paid" || asInt(rows[0]["total"]) != 20 {
			t.Fatalf("version gate: want v2 payload (paid,20) to survive an older v1 write, got %+v", rows[0])
		}
	})

	t.Run("ProjectedFactsDimensionMustBeDeclared", func(t *testing.T) {
		q := newQ(reg)
		var rows []map[string]any
		err := q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"ssn"}, // not in the "order" InsightsSpec
			Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows)
		if err == nil {
			t.Fatal("want error: dimension \"ssn\" is not declared in the order InsightsSpec")
		}
	})

	t.Run("ProjectedFactsFilterMustBeDeclared", func(t *testing.T) {
		// Pins the fake's checkAllowedInWhere (fabriqtest/analytics_query_fake.go),
		// flagged as untested in the Task 6 report: a Filter column not in the
		// "order" InsightsSpec allow-list must be rejected, mirroring the
		// dimension/measure checks above and the real adapter's
		// mapCondToProp check.
		q := newQ(reg)
		var rows []map[string]any
		err := q.Query(ctx1, query.AnalyticsQuery{
			Source:   "order",
			Filter:   query.Where{query.Eq("ssn", "123-45-6789")}, // not in the "order" InsightsSpec
			Measures: []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		}, &rows)
		if err == nil {
			t.Fatal("want error: filter column \"ssn\" is not declared in the order InsightsSpec")
		}
	})

	// --- Metric-by-name (Task 7): Query.Source names a declared MetricSpec ---
	//
	// "revenue" (registered above, alongside the "order" InsightsSpec entity)
	// sources "order" facts: Sum(amount) As "rev", grouped by status. These
	// subtests upsert their OWN facts (fresh agg IDs) rather than reusing the
	// ones QueryProjectedFacts/ProjectedFactsVersionGate already wrote, so
	// they don't depend on subtest ordering or on those facts still being
	// there — but since facts are cumulative across subtests sharing reg,
	// the equivalence check below (MetricByNameExpands) reads through the
	// SAME facts an explicit-measure query would, over the entity's full
	// surviving fact set.

	t.Run("MetricByNameExpands", func(t *testing.T) {
		q := newQ(reg)
		sink, ok := q.(FactSink)
		if !ok {
			t.Fatalf("%T does not implement insights.FactSink", q)
		}
		must(t, sink.UpsertInsightFacts(ctx1, []Fact{
			{TenantID: "t1", Entity: "order", AggID: "m1", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":10}`)},
			{TenantID: "t1", Entity: "order", AggID: "m2", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":5}`)},
			{TenantID: "t1", Entity: "order", AggID: "m3", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"void","amount":0}`)},
		}))

		var byMetric []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{Source: "revenue"}, &byMetric))

		var explicit []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source:     "order",
			Dimensions: []string{"status"},
			Measures:   []query.Measure{{Kind: query.MeasureSum, Field: "amount", As: "rev"}},
		}, &explicit))

		paid := findByStatus(byMetric, "paid")
		if paid == nil || asInt(paid["rev"]) != 15 {
			t.Fatalf("metric-by-name paid group wrong: %+v", paid)
		}
		if len(byMetric) != len(explicit) {
			t.Fatalf("metric-by-name and explicit-measure query disagree on group count: %d vs %d", len(byMetric), len(explicit))
		}
		for _, er := range explicit {
			mr := findByStatus(byMetric, er["status"])
			if mr == nil || asInt(mr["rev"]) != asInt(er["rev"]) {
				t.Fatalf("metric-by-name diverges from explicit-measure equivalent for status %v: metric=%+v explicit=%+v", er["status"], mr, er)
			}
		}
	})

	t.Run("MetricCallerFilterApplies", func(t *testing.T) {
		q := newQ(reg)
		sink, ok := q.(FactSink)
		if !ok {
			t.Fatalf("%T does not implement insights.FactSink", q)
		}
		must(t, sink.UpsertInsightFacts(ctx1, []Fact{
			{TenantID: "t1", Entity: "order", AggID: "mf1", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":10}`)},
			{TenantID: "t1", Entity: "order", AggID: "mf2", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"paid","amount":5}`)},
			{TenantID: "t1", Entity: "order", AggID: "mf3", Version: 1, At: base,
				Payload: json.RawMessage(`{"status":"void","amount":0}`)},
		}))
		var rows []map[string]any
		must(t, q.Query(ctx1, query.AnalyticsQuery{
			Source: "revenue",
			Filter: query.Where{query.Eq("status", "paid")},
		}, &rows))
		if len(rows) != 1 || rows[0]["status"] != "paid" || asInt(rows[0]["rev"]) != 15 {
			t.Fatalf("metric caller-filter wrong: %+v", rows)
		}
	})

	t.Run("MetricRejectsExplicitMeasures", func(t *testing.T) {
		q := newQ(reg)
		var rows []map[string]any
		err := q.Query(ctx1, query.AnalyticsQuery{
			Source:   "revenue",
			Measures: []query.Measure{{Kind: query.MeasureCount}},
		}, &rows)
		if err == nil {
			t.Fatal("want error: metric + explicit Measures must be rejected")
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

// findByStatus returns the first row whose "status" column equals value, or nil.
func findByStatus(rows []map[string]any, value any) map[string]any {
	for _, r := range rows {
		if r["status"] == value {
			return r
		}
	}
	return nil
}

// toFloatT coerces common numeric representations to float64. Beyond the
// JSON-round-trip shapes (float64, json.Number, Go-native ints/floats before
// one), it also handles what a real SQL adapter's map-scan can hand back for
// a SUM/AVG/MIN/MAX over a numeric column:
//
//   - string / []byte: some drivers scan `numeric` as text.
//   - a decimal type that implements json.Marshaler (e.g. pgx's
//     pgtype.Numeric, which round-trips through MarshalJSON as a bare
//     number token, not a quoted string): marshal it and parse the result.
//     This keeps core/insights free of any driver import (the dependency
//     fence — core never imports a specific adapter's driver package) while
//     still being able to read whatever numeric shape that driver produces.
//
// This is representation-robustness only: it never changes what value is
// considered correct, only what Go types are accepted to carry it.
func toFloatT(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	case []byte:
		f, err := strconv.ParseFloat(strings.TrimSpace(string(n)), 64)
		return f, err == nil
	default:
		// Decimal types that don't natively satisfy any of the above (e.g.
		// pgx's pgtype.Numeric) still implement json.Marshaler and emit a
		// bare numeric JSON token for a valid, non-NaN value. Use that
		// instead of importing the driver package directly.
		if m, ok := v.(interface{ MarshalJSON() ([]byte, error) }); ok {
			buf, err := m.MarshalJSON()
			if err != nil {
				return 0, false
			}
			f, perr := strconv.ParseFloat(strings.TrimSpace(string(buf)), 64)
			return f, perr == nil
		}
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
