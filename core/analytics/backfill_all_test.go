package analytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestBackfillAll_TwoTenants(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	// snapshot returns a row whose tenant matches the requested tenantID
	snap := func(_ context.Context, tenantID string, fn func(event.Envelope) error) error {
		return fn(event.Envelope{TenantID: tenantID, Aggregate: "widget", AggID: "w1", Version: 1, Type: "widget.updated", Payload: []byte(`{"name":"a"}`)})
	}
	b := &analytics.Backfiller{Snapshot: snap,
		Applier: analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})), Sink: sink}
	counts, err := b.AllTenants(context.Background(), []string{"t1", "t2"}, 2)
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if counts["t1"] != 1 || counts["t2"] != 1 {
		t.Fatalf("counts=%v", counts)
	}
	if _, ok := sink.Facts()["t1|widget|w1"]; !ok {
		t.Fatal("t1 missing")
	}
	if _, ok := sink.Facts()["t2|widget|w1"]; !ok {
		t.Fatal("t2 missing")
	}
}
