package insights_test

import (
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// resolveOrderModel gives the "order"/"plain" test entities a distinct table
// name so this file can register its own fixtures without colliding with
// other _test.go files' model tables in core/registry (see
// core/registry/metric_index_test.go's analogous metricOrderModel).
type resolveOrderModel struct {
	grove.BaseModel `grove:"table:resolve_orders"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Amount   int64  `grove:"amount"`
	Status   string `grove:"status"`
}

type resolvePlainModel struct {
	grove.BaseModel `grove:"table:resolve_plains"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name"`
}

// testRegistry builds a registry with an InsightsSpec entity "order"
// (Measures amount, Dimensions status), a plain entity "plain" with no
// InsightsSpec, and a metric "revenue" sourcing "order". "signup" is
// deliberately never registered as an entity, so it resolves as an event.
func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name:  "order",
		Model: (*resolveOrderModel)(nil),
		Insights: &registry.InsightsSpec{
			Measures:   []string{"amount"},
			Dimensions: []string{"status"},
		},
		Metrics: []registry.MetricSpec{{
			Name:       "revenue",
			Source:     "order",
			Measures:   []registry.MetricMeasure{{Kind: "sum", Field: "amount"}},
			Dimensions: []string{"status"},
		}},
	}); err != nil {
		t.Fatalf("register order: %v", err)
	}
	if err := r.Register(registry.EntitySpec{
		Name:  "plain",
		Model: (*resolvePlainModel)(nil),
	}); err != nil {
		t.Fatalf("register plain: %v", err)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return r
}

func TestResolveSource_Event(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "signup") // not an entity, not a metric
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != insights.SourceEvent || d.Table != "fabriq_insights_events" || d.JSONColumn != "props" || d.KeyColumn != "name" || d.KeyValue != "signup" || d.AllowedColumns != nil {
		t.Fatalf("event descriptor wrong: %+v", d)
	}
}

func TestResolveSource_Facts(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "order")
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != insights.SourceFacts || d.Table != "fabriq_insights_facts" || d.JSONColumn != "payload" || d.KeyColumn != "entity" || d.ExtraWhere != "deleted = false" {
		t.Fatalf("facts descriptor wrong: %+v", d)
	}
	if !d.AllowedColumns["amount"] || !d.AllowedColumns["status"] || d.AllowedColumns["ssn"] {
		t.Fatalf("facts AllowedColumns wrong: %+v", d.AllowedColumns)
	}
}

func TestResolveSource_MetricExpandsToFacts(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "revenue")
	if err != nil {
		t.Fatal(err)
	}
	if !d.FromMetric || d.Kind != insights.SourceFacts {
		t.Fatalf("metric->facts descriptor wrong: %+v", d)
	}
	if d.MetricName != "revenue" {
		t.Fatalf("metric name wrong: %+v", d)
	}
	if len(d.MetricMeasures) != 1 || d.MetricMeasures[0].Kind != "sum" || d.MetricMeasures[0].Field != "amount" {
		t.Fatalf("metric measures wrong: %+v", d.MetricMeasures)
	}
	if len(d.MetricDimensions) != 1 || d.MetricDimensions[0] != "status" {
		t.Fatalf("metric dims wrong: %+v", d.MetricDimensions)
	}
}

func TestResolveSource_EntityWithoutInsightsIsError(t *testing.T) {
	reg := testRegistry(t) // also registers a plain entity "plain" with no InsightsSpec
	if _, err := insights.ResolveSource(reg, "plain"); err == nil {
		t.Fatal("want error: entity has no InsightsSpec")
	}
}

func TestResolveSource_NilRegistryIsEvent(t *testing.T) {
	d, err := insights.ResolveSource(nil, "anything")
	if err != nil {
		t.Fatal(err)
	}
	if d.Kind != insights.SourceEvent || d.KeyValue != "anything" {
		t.Fatalf("nil-registry descriptor wrong: %+v", d)
	}
}

func TestResolveSource_MetricSourcingMetricIsError(t *testing.T) {
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name:  "order",
		Model: (*resolveOrderModel)(nil),
		Insights: &registry.InsightsSpec{
			Measures:   []string{"amount"},
			Dimensions: []string{"status"},
		},
		Metrics: []registry.MetricSpec{
			{Name: "revenue", Source: "order", Measures: []registry.MetricMeasure{{Kind: "sum", Field: "amount"}}},
			{Name: "revenue_daily", Source: "revenue", Measures: []registry.MetricMeasure{{Kind: "sum", Field: "amount"}}},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := insights.ResolveSource(r, "revenue_daily"); err == nil {
		t.Fatal("want error: metric sourcing another metric is not supported")
	}
}

func TestEffectiveQuery_NonMetricPassesThrough(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "order")
	if err != nil {
		t.Fatal(err)
	}
	q := query.AnalyticsQuery{
		Source:     "order",
		Measures:   []query.Measure{{Kind: query.MeasureCount, As: "n"}},
		Dimensions: []string{"status"},
		TimeBucket: time.Hour,
	}
	measures, dims, bucket, err := insights.EffectiveQuery(q, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(measures) != 1 || measures[0].As != "n" {
		t.Fatalf("measures wrong: %+v", measures)
	}
	if len(dims) != 1 || dims[0] != "status" {
		t.Fatalf("dims wrong: %+v", dims)
	}
	if bucket != time.Hour {
		t.Fatalf("bucket wrong: %v", bucket)
	}
}

func TestEffectiveQuery_MetricExpands(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "revenue")
	if err != nil {
		t.Fatal(err)
	}
	measures, dims, bucket, err := insights.EffectiveQuery(query.AnalyticsQuery{Source: "revenue"}, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(measures) != 1 || measures[0].Kind != query.MeasureSum || measures[0].Field != "amount" {
		t.Fatalf("expanded measures wrong: %+v", measures)
	}
	if len(dims) != 1 || dims[0] != "status" {
		t.Fatalf("expanded dims wrong: %+v", dims)
	}
	if bucket != 0 {
		t.Fatalf("expanded bucket wrong: %v", bucket)
	}
}

func TestEffectiveQuery_MetricCallerBucketOverridesDefault(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "revenue")
	if err != nil {
		t.Fatal(err)
	}
	_, _, bucket, err := insights.EffectiveQuery(query.AnalyticsQuery{Source: "revenue", TimeBucket: time.Minute}, d)
	if err != nil {
		t.Fatal(err)
	}
	if bucket != time.Minute {
		t.Fatalf("caller TimeBucket should override metric default: %v", bucket)
	}
}

func TestEffectiveQuery_MetricRejectsExplicitMeasures(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "revenue")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := insights.EffectiveQuery(query.AnalyticsQuery{
		Source:   "revenue",
		Measures: []query.Measure{{Kind: query.MeasureCount}},
	}, d); err == nil {
		t.Fatal("want error: metric + explicit measures")
	}
}

func TestEffectiveQuery_MetricRejectsExplicitDimensions(t *testing.T) {
	reg := testRegistry(t)
	d, err := insights.ResolveSource(reg, "revenue")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := insights.EffectiveQuery(query.AnalyticsQuery{
		Source:     "revenue",
		Dimensions: []string{"status"},
	}, d); err == nil {
		t.Fatal("want error: metric + explicit dimensions")
	}
}
