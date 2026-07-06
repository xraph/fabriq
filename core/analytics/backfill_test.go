package analytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func snapshotOf(envs ...event.Envelope) analytics.SnapshotFunc {
	return func(_ context.Context, _ string, fn func(event.Envelope) error) error {
		for _, e := range envs {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestBackfill_MaterializesMarkedRows(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	b := &analytics.Backfiller{
		Snapshot: snapshotOf(
			env("widget", "widget.updated", 4, `{"name":"a","ssn":"x"}`),
		),
		Applier: analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})),
		Sink:    sink,
	}
	n, err := b.Tenant(context.Background(), "t1")
	if err != nil || n != 1 {
		t.Fatalf("backfill n=%d err=%v", n, err)
	}
	if sink.Facts()["t1|widget|w1"].Version != 4 {
		t.Fatal("fact not backfilled at version 4")
	}
}

func TestBackfill_IdempotentWithLiveVersion(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	_ = sink.UpsertFacts(context.Background(), []analytics.Fact{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 9, Payload: []byte(`{}`)}})
	b := &analytics.Backfiller{
		Snapshot: snapshotOf(env("widget", "widget.updated", 4, `{"name":"a"}`)), // older snapshot
		Applier:  analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})),
		Sink:     sink,
	}
	_, _ = b.Tenant(context.Background(), "t1")
	if sink.Facts()["t1|widget|w1"].Version != 9 {
		t.Fatal("backfill must not regress a newer live fact (version gate)")
	}
}
