package analytics_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// snapshotEnvs replays a fixed set of current-state envelopes for any tenant.
func snapshotEnvs(envs ...event.Envelope) analytics.SnapshotFunc {
	return func(_ context.Context, _ string, fn func(event.Envelope) error) error {
		for _, e := range envs {
			if err := fn(e); err != nil {
				return err
			}
		}
		return nil
	}
}

func recEnv(aggID string, v int64) event.Envelope {
	return event.Envelope{
		TenantID: "t1", Aggregate: "widget", AggID: aggID, Version: v,
		Type: "widget.updated", At: time.Unix(100, 0).UTC(),
		Payload: json.RawMessage(`{"name":"n"}`),
	}
}

func TestReconciler_HealsMissingAndStale(t *testing.T) {
	ctx := context.Background()
	sink := fabriqtest.NewFakeAnalyticsSink()
	reg := regWith(&registry.AnalyticsSpec{Include: []string{"name"}})

	// Pre-seed the sink as if the consumer had applied w1@2 but SKIPPED w2 (poison)
	// and only got w3 up to version 1 while the source is at 2.
	_ = sink.UpsertFacts(ctx, []analytics.Fact{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 2, Payload: json.RawMessage(`{"name":"n"}`), At: time.Unix(100, 0).UTC()}})
	_ = sink.SetWatermark(ctx, []analytics.Watermark{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 2}})
	_ = sink.UpsertFacts(ctx, []analytics.Fact{{TenantID: "t1", Aggregate: "widget", AggID: "w3", Version: 1, Payload: json.RawMessage(`{"name":"n"}`), At: time.Unix(100, 0).UTC()}})
	_ = sink.SetWatermark(ctx, []analytics.Watermark{{TenantID: "t1", Aggregate: "widget", AggID: "w3", Version: 1}})

	// Source of truth: w1@2 (current), w2@1 (missing from analytics), w3@2 (stale).
	rc := &analytics.Reconciler{
		Snapshot: snapshotEnvs(recEnv("w1", 2), recEnv("w2", 1), recEnv("w3", 2)),
		Applier:  analytics.NewApplier(reg),
		Sink:     sink,
	}

	rep, err := rc.Tenant(ctx, "t1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rep.Checked != 3 {
		t.Fatalf("checked = %d, want 3", rep.Checked)
	}
	if rep.Missing != 1 || rep.Stale != 1 || rep.Healed != 2 {
		t.Fatalf("report = %+v, want Missing=1 Stale=1 Healed=2", rep)
	}
	if rep.Drifted() != 2 {
		t.Fatalf("drifted = %d, want 2", rep.Drifted())
	}

	// w2 is now present, w3 advanced to version 2.
	if _, ok := sink.Facts()["t1|widget|w2"]; !ok {
		t.Fatal("w2 was not healed into the sink")
	}
	if v, _ := sink.Watermark(ctx, "t1", "widget", "w3"); v != 2 {
		t.Fatalf("w3 watermark = %d, want 2 after heal", v)
	}

	// Idempotent: a second reconcile finds no drift.
	rep2, err := rc.Tenant(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Drifted() != 0 || rep2.Healed != 0 {
		t.Fatalf("second reconcile = %+v, want no drift", rep2)
	}
}

func TestReconciler_SkipsUnmarked(t *testing.T) {
	ctx := context.Background()
	sink := fabriqtest.NewFakeAnalyticsSink()
	rc := &analytics.Reconciler{
		Snapshot: snapshotEnvs(recEnv("w1", 1)),
		Applier:  analytics.NewApplier(regWith(nil)), // unmarked
		Sink:     sink,
	}
	rep, err := rc.Tenant(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checked != 0 || rep.Healed != 0 {
		t.Fatalf("unmarked reconcile = %+v, want all zero", rep)
	}
}
