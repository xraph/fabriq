//go:build integration

package chanalytics_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/chanalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

// noCloseSink shares one *chanalytics.Sink across the suite; RunSinkConformance's
// per-sub-test Close must not tear down the shared connection. Each newSink()
// truncates for isolation (mirrors the pganalytics conformance harness).
type noCloseSink struct{ *chanalytics.Sink }

func (noCloseSink) Close() error { return nil }

func TestChAnalytics_Conformance(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartClickHouse(t)
	s, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	analytics.RunSinkConformance(t, func() analytics.Sink {
		if err := chanalytics.TruncateForTest(ctx, s); err != nil {
			t.Fatal(err)
		}
		return noCloseSink{s}
	})
}

func TestChAnalytics_ConcurrentUpsert(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartClickHouse(t)
	s, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := chanalytics.TruncateForTest(ctx, s); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := int64(1); i <= 50; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			_ = s.UpsertFacts(ctx, []analytics.Fact{{
				TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: v,
				Payload: []byte(`{}`), At: time.Unix(100, 0).UTC(),
			}})
		}(i)
	}
	wg.Wait()

	// The version-gate is resolved at read time by argMax over _dedup: the
	// highest version must win regardless of insert interleaving.
	got, err := chanalytics.LatestFactVersionForTest(ctx, s, "t1", "widget", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if got != 50 {
		t.Fatalf("concurrent upsert: fact version=%d want 50", got)
	}
}

func TestChAnalytics_ReprojectThenAppendThenPrune(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartClickHouse(t)
	s, err := chanalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := chanalytics.TruncateForTest(ctx, s); err != nil {
		t.Fatal(err)
	}

	at := time.Unix(1000, 0).UTC()
	f := analytics.Fact{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 1,
		Payload: json.RawMessage(`{"keep":"y","drop":"secret"}`), At: at}
	e := analytics.Event{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 1, Type: "widget.created",
		Payload: json.RawMessage(`{"keep":"y","drop":"secret"}`), At: at}
	if err := s.UpsertFacts(ctx, []analytics.Fact{f}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents(ctx, []analytics.Event{e}); err != nil {
		t.Fatal(err)
	}

	keepOnly := func(p json.RawMessage) (json.RawMessage, error) {
		var in map[string]json.RawMessage
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		if v, ok := in["keep"]; ok {
			return json.RawMessage(`{"keep":` + string(v) + `}`), nil
		}
		return json.RawMessage(`{}`), nil
	}

	// Reproject: rewrites the fact and the event, creating a second physical
	// _dedup row for the one logical event.
	n, err := s.ReprojectTenant(ctx, "t1", "widget", keepOnly)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("reproject rewrote %d rows, want 2 (fact + event)", n)
	}

	// At-least-once redelivery: re-append the SAME event key — a third physical
	// row for the same logical event (dedup no-op).
	if err := s.AppendEvents(ctx, []analytics.Event{e}); err != nil {
		t.Fatal(err)
	}

	// Prune by age: the ONE logical event is older than the cutoff. The count
	// must be logical (1), not inflated by the reprojection/redelivery rows.
	pruned, err := s.PruneEvents(ctx, time.Unix(5000, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("pruned %d logical events, want 1 (physical _dedup/redelivery rows must not inflate the count)", pruned)
	}

	// Idempotent: re-prune at the same cutoff removes nothing.
	if again, err := s.PruneEvents(ctx, time.Unix(5000, 0).UTC()); err != nil || again != 0 {
		t.Fatalf("re-prune = %d err=%v, want 0/nil", again, err)
	}
}
