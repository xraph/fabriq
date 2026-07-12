package forgeext

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type imodel struct {
	grove.BaseModel `grove:"table:imodels"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Amount          int64  `grove:"amount"`
}

// TestHasInsightsEntity mirrors TestHasAnalyticsEntity: hasInsightsEntity
// gates the proj:insights supervise block (worker.go, sweeper.go) the same
// way hasAnalyticsEntity gates proj:analytics.
func TestHasInsightsEntity(t *testing.T) {
	empty := registry.New()
	if err := empty.Register(registry.EntitySpec{Name: "a", Kind: registry.KindAggregate, Model: (*imodel)(nil)}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if hasInsightsEntity(empty) {
		t.Fatal("no marked entity => false")
	}
	marked := registry.New()
	if err := marked.Register(registry.EntitySpec{Name: "a", Kind: registry.KindAggregate, Model: (*imodel)(nil), Insights: &registry.InsightsSpec{Measures: []string{"amount"}}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !hasInsightsEntity(marked) {
		t.Fatal("marked entity => true")
	}
}
