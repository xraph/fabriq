package registry_test

import (
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type insightModel struct {
	grove.BaseModel `grove:"table:insight_orders"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Amount   int64  `grove:"amount"`
	Status   string `grove:"status"`
}

func TestInsightsSpec_RejectsUnknownDimensionColumn(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name:  "order",
		Model: (*insightModel)(nil),
		Insights: &registry.InsightsSpec{
			Measures:   []string{"amount"},
			Dimensions: []string{"nonexistent"},
		},
	})
	if err := firstErr(r, err); err == nil || !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("want error naming the unknown column, got %v", err)
	}
}

func TestInsightsSpec_RejectsEmpty(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name:     "order",
		Model:    (*insightModel)(nil),
		Insights: &registry.InsightsSpec{}, // no Measures, no Dimensions
	})
	if err == nil || !strings.Contains(err.Error(), "Insights") {
		t.Fatalf("want empty-Insights rejection, got %v", err)
	}
}

func TestMetricSpec_RejectsBadMeasureField(t *testing.T) {
	r := registry.New()
	_ = r.Register(registry.EntitySpec{Name: "order", Model: (*insightModel)(nil)})
	err := r.Register(registry.EntitySpec{
		Name:  "order2",
		Model: (*insightModel)(nil),
		Metrics: []registry.MetricSpec{{
			Name: "revenue", Measures: []registry.MetricMeasure{{Kind: "sum", Field: "ghost"}},
		}},
	})
	if err := firstErr(r, err); err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("want error naming the unknown measure field, got %v", err)
	}
}

// firstErr returns the register error, else r.Validate(), so a test can assert
// on whichever stage catches the fault.
func firstErr(r *registry.Registry, regErr error) error {
	if regErr != nil {
		return regErr
	}
	return r.Validate()
}
