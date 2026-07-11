package fabriq

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

// TestStores_InsightsConsumer_NeedsRedis mirrors AnalyticsConsumer's
// contract: the proj:insights consumer needs the shared Redis stream (its
// projection.Source) regardless of tenancy mode, so a Stores value with no
// Redis configured must fail fast rather than build a consumer that can
// never run. Building a live *redis.Adapter needs a real connection, so this
// is the boundary this unit test can reach without Docker — the routing
// mechanism itself (per-tenant FactSink resolution via shard.NewAnalytics)
// is covered by adapters/shard/insights_test.go, which fakes the shard set
// directly.
func TestStores_InsightsConsumer_NeedsRedis(t *testing.T) {
	reg := registry.New()
	s := &Stores{}
	cons, err := s.InsightsConsumer(reg)
	if err == nil {
		t.Fatal("expected an error when redis is not configured")
	}
	if cons != nil {
		t.Fatal("expected a nil consumer alongside the error")
	}
}
