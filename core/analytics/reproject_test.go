package analytics_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/registry"
)

func TestReprojector_AppliesTightenedAllowList(t *testing.T) {
	ctx := context.Background()
	sink := fabriqtest.NewFakeAnalyticsSink()

	// Seed as if written under a WIDER allow-list: payload still carries "ssn".
	seed := analytics.Fact{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 3,
		Payload: json.RawMessage(`{"name":"a","ssn":"x"}`), At: time.Unix(100, 0).UTC()}
	seedEv := analytics.Event{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 3,
		Type: "widget.updated", Payload: json.RawMessage(`{"name":"a","ssn":"x"}`), At: time.Unix(100, 0).UTC()}
	_ = sink.UpsertFacts(ctx, []analytics.Fact{seed})
	_ = sink.AppendEvents(ctx, []analytics.Event{seedEv})

	// Current spec is now NARROWER: only "name" is allow-listed.
	rp := &analytics.Reprojector{Reg: regWith(&registry.AnalyticsSpec{Include: []string{"name"}}), Sink: sink}

	n, err := rp.Tenant(ctx, "t1")
	if err != nil {
		t.Fatalf("reproject: %v", err)
	}
	if n != 2 { // fact + event
		t.Fatalf("rewrote %d rows, want 2", n)
	}

	// "ssn" must be gone from the stored fact.
	var got map[string]any
	_ = json.Unmarshal(sink.Facts()["t1|widget|w1"].Payload, &got)
	if _, leaked := got["ssn"]; leaked {
		t.Fatalf("ssn survived reprojection: %v", got)
	}
	if got["name"] != "a" {
		t.Fatalf("name lost in reprojection: %v", got)
	}

	// Idempotent: nothing left to narrow.
	again, err := rp.Tenant(ctx, "t1")
	if err != nil || again != 0 {
		t.Fatalf("second reproject: n=%d err=%v, want 0/nil", again, err)
	}
}

func TestReprojector_SkipsUnmarkedEntities(t *testing.T) {
	ctx := context.Background()
	sink := fabriqtest.NewFakeAnalyticsSink()
	// An unmarked registry: nothing is analyticized, so nothing is reprojected.
	rp := &analytics.Reprojector{Reg: regWith(nil), Sink: sink}
	n, err := rp.Tenant(ctx, "t1")
	if err != nil || n != 0 {
		t.Fatalf("unmarked reproject: n=%d err=%v, want 0/nil", n, err)
	}
}

type countingReprojectSink struct {
	*fabriqtest.FakeAnalyticsSink
	writes int
}

func (s *countingReprojectSink) ReprojectTenant(ctx context.Context, tenant, aggregate string, transform func(json.RawMessage) (json.RawMessage, error)) (int64, error) {
	s.writes++
	return s.FakeAnalyticsSink.ReprojectTenant(ctx, tenant, aggregate, transform)
}

func TestReprojectorRefusesHashPoliciesBeforeAnyWrite(t *testing.T) {
	r := regWith(&registry.AnalyticsSpec{Include: []string{"name"}})
	if err := r.Register(registry.EntitySpec{Name: "private_widget", Kind: registry.KindAggregate, Schema: &registry.DynamicSchema{Table: "ds_private_widgets", Columns: []registry.DynamicColumn{{Name: "ssn", Type: registry.ColText}}}, Analytics: &registry.AnalyticsSpec{Hash: []string{"ssn"}}}); err != nil {
		t.Fatal(err)
	}
	sink := &countingReprojectSink{FakeAnalyticsSink: fabriqtest.NewFakeAnalyticsSink()}
	rp := &analytics.Reprojector{Reg: r, Sink: sink, Salt: "stable-secret"}
	n, err := rp.Tenant(context.Background(), "acme")
	if err == nil || n != 0 || sink.writes != 0 {
		t.Fatalf("hash reprojection must fail before any writes: n=%d calls=%d err=%v", n, sink.writes, err)
	}
}
