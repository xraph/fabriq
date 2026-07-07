//go:build integration

package chanalytics_test

import (
	"context"
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
