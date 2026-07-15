package forgeext

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type rollupOwnerModel struct {
	grove.BaseModel `grove:"table:rollup_owner_models"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Amount          int64  `grove:"amount"`
}

// TestRollupJobGate_HasMaterializedMetric mirrors TestHasAnalyticsEntity/
// TestHasInsightsEntity: hasMaterializedMetric gates the rollup:insights
// supervise block (worker.go, sweeper.go) the same way hasAnalyticsEntity/
// hasInsightsEntity gate their own jobs. MaterializedMetrics() reads an
// index Validate builds, so both cases call Validate before asserting.
func TestRollupJobGate_HasMaterializedMetric(t *testing.T) {
	noMetrics := registry.New()
	if err := noMetrics.Register(registry.EntitySpec{
		Name: "owner", Kind: registry.KindAggregate, Model: (*rollupOwnerModel)(nil),
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := noMetrics.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if hasMaterializedMetric(noMetrics) {
		t.Fatal("no metrics => false")
	}

	liveOnly := registry.New()
	if err := liveOnly.Register(registry.EntitySpec{
		Name: "owner", Kind: registry.KindAggregate, Model: (*rollupOwnerModel)(nil),
		Metrics: []registry.MetricSpec{
			{Name: "checkouts_live", Source: "checkout", Measures: []registry.MetricMeasure{{Kind: "count", As: "n"}}},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := liveOnly.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if hasMaterializedMetric(liveOnly) {
		t.Fatal("a metric with no Rollup => false")
	}

	materialized := registry.New()
	if err := materialized.Register(registry.EntitySpec{
		Name: "owner", Kind: registry.KindAggregate, Model: (*rollupOwnerModel)(nil),
		Metrics: []registry.MetricSpec{
			{
				Name: "checkouts_rollup", Source: "checkout",
				Measures: []registry.MetricMeasure{{Kind: "count", As: "n"}},
				Rollup:   &registry.RollupSpec{Bucket: time.Hour},
			},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := materialized.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !hasMaterializedMetric(materialized) {
		t.Fatal("a metric with Rollup set => true")
	}
}

// TestRunRollupMaintainer_NoopWithoutStores proves the job body degrades to
// a no-op (not a panic) when called before Start wires e.stores — the same
// defensive shape runReconciler/runBlobGC rely on via their own nil-stores
// guards in reconcileAll/gcBlobAll.
func TestRunRollupMaintainer_NoopWithoutStores(t *testing.T) {
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name: "owner", Kind: registry.KindAggregate, Model: (*rollupOwnerModel)(nil),
		Metrics: []registry.MetricSpec{
			{
				Name: "checkouts_rollup", Source: "checkout",
				Measures: []registry.MetricMeasure{{Kind: "count", As: "n"}},
				Rollup:   &registry.RollupSpec{Bucket: time.Hour},
			},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	ext := New(reg)
	// e.stores is nil (Start was never called) — runRollupMaintainer must
	// return immediately rather than dereferencing it.
	done := make(chan struct{})
	go func() {
		ext.runRollupMaintainer(context.Background(), time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRollupMaintainer did not return promptly with nil stores")
	}
}
