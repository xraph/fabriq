package registry_test

import (
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

// metricOrderModel mirrors insightModel (see insights_spec_test.go) under a
// distinct table name so both test files can register same-shaped fixtures
// without a table-name collision.
type metricOrderModel struct {
	grove.BaseModel `grove:"table:metric_orders"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Amount   int64  `grove:"amount"`
	Status   string `grove:"status"`
}

// orderModel returns a fresh Model value suitable for any EntitySpec in this
// file — the test cases don't care about its shape beyond having amount/status
// columns for the Insights/Metric fixtures.
func orderModel() any {
	return (*metricOrderModel)(nil)
}

// mustRegisterOrder registers an entity "order" with an InsightsSpec over
// amount/status plus a metric "revenue" (sum of amount, grouped by status)
// sourcing "order".
func mustRegisterOrder(t *testing.T, r *registry.Registry) {
	t.Helper()
	if err := r.Register(registry.EntitySpec{
		Name:  "order",
		Model: orderModel(),
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
		t.Fatalf("mustRegisterOrder: %v", err)
	}
}

// minimalDynSchema returns a minimal, valid DynamicSchema for entities in this
// file that only need SOME distinct binding to hang Metrics off of (no shared
// Go model type, so each entity gets its own registry.Registry.byModel slot;
// named to avoid colliding with dynamic_test.go's own dynSpec helper).
func minimalDynSchema(table string) *registry.DynamicSchema {
	return &registry.DynamicSchema{Table: table}
}

func TestValidate_RejectsMetricNameCollidingWithEntity(t *testing.T) {
	r := registry.New()
	mustRegisterOrder(t, r) // helper: registers entity "order" with an InsightsSpec over amount/status
	err := r.Register(registry.EntitySpec{
		Name:   "widget",
		Schema: minimalDynSchema("widgets"), // any valid entity binding
		Metrics: []registry.MetricSpec{{
			Name: "order", Source: "order_placed", // metric name collides with entity "order"
			Measures: []registry.MetricMeasure{{Kind: "count"}},
		}},
	})
	got := firstErr(r, err) // register-or-Validate, as in insights_spec_test.go
	if got == nil || !strings.Contains(got.Error(), "order") {
		t.Fatalf("want metric/entity name collision error, got %v", got)
	}
}

func TestValidate_RejectsDuplicateMetricNames(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name: "a", Schema: minimalDynSchema("as"),
		Metrics: []registry.MetricSpec{{Name: "revenue", Source: "sale", Measures: []registry.MetricMeasure{{Kind: "count"}}}},
	})
	err2 := r.Register(registry.EntitySpec{
		Name: "b", Schema: minimalDynSchema("bs"),
		Metrics: []registry.MetricSpec{{Name: "revenue", Source: "sale", Measures: []registry.MetricMeasure{{Kind: "count"}}}},
	})
	if e := firstErr2(r, err, err2); e == nil || !strings.Contains(e.Error(), "revenue") {
		t.Fatalf("want duplicate metric name error, got %v", e)
	}
}

func TestValidate_RejectsMetricEntitySourceWithoutInsights(t *testing.T) {
	r := registry.New()
	// "plain" entity has NO InsightsSpec; a metric sourcing it must be rejected.
	_ = r.Register(registry.EntitySpec{Name: "plain", Schema: minimalDynSchema("plains")})
	_ = r.Register(registry.EntitySpec{
		Name: "m", Schema: minimalDynSchema("ms"),
		Metrics: []registry.MetricSpec{{Name: "x", Source: "plain", Measures: []registry.MetricMeasure{{Kind: "count"}}}},
	})
	if e := r.Validate(); e == nil || !strings.Contains(e.Error(), "plain") {
		t.Fatalf("want error: metric source entity lacks InsightsSpec, got %v", e)
	}
}

func TestMetricAndEntityAccessors(t *testing.T) {
	r := registry.New()
	mustRegisterOrder(t, r) // entity "order" + InsightsSpec, and a metric "revenue" sourcing "order"
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !r.EntityHasInsights("order") {
		t.Fatal("EntityHasInsights(order) should be true")
	}
	if r.EntityHasInsights("nope") {
		t.Fatal("EntityHasInsights(nope) should be false")
	}
	if _, ok := r.Metric("revenue"); !ok {
		t.Fatal("Metric(revenue) should resolve")
	}
	if _, ok := r.Metric("order"); ok {
		t.Fatal("Metric(order) should NOT resolve (order is an entity, not a metric)")
	}
}

// firstErr2 returns the first non-nil of errs, else r.Validate(), mirroring
// firstErr (insights_spec_test.go) for call sites with two register errors.
func firstErr2(r *registry.Registry, errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return r.Validate()
}
