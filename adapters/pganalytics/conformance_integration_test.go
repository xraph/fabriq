//go:build integration

package pganalytics_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

// noCloseSink wraps a shared *pganalytics.Sink so RunSinkConformance's
// per-sub-test `defer s.Close()` doesn't tear down the pool the other
// sub-tests still need. The suite treats every newSink() call as owning an
// independent Sink (true for the fake, whose Close is a no-op); the adapter
// only affords one open pool per DSN, so it is shared and truncated instead.
type noCloseSink struct{ *pganalytics.Sink }

func (noCloseSink) Close() error { return nil }

func TestPgAnalytics_Conformance(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)
	ctx := context.Background()
	// Open once; each newSink() call truncates so sub-tests are isolated
	// without paying for a fresh container/schema per sub-test.
	s, err := pganalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	analytics.RunSinkConformance(t, func() analytics.Sink {
		if err := pganalytics.TruncateForTest(ctx, s); err != nil {
			t.Fatal(err)
		}
		return noCloseSink{s}
	})
}

func TestPgAnalytics_ConcurrentUpsert(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	s, err := pganalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := pganalytics.TruncateForTest(ctx, s); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := int64(1); i <= 50; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			_ = s.UpsertFacts(ctx, []analytics.Fact{{
				TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: v,
				Payload: []byte(`{}`), At: time.Now(),
			}})
		}(i)
	}
	wg.Wait()

	// Highest version must win regardless of interleaving. Read the fact row
	// directly (not via SetWatermark, which is a separate table/mechanism)
	// to prove the version-gate held under concurrent writers.
	raw := pgdriver.New()
	if err := raw.Open(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rows, err := raw.Query(ctx, `SELECT version FROM fabriq_analytics_facts WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "t1", "widget", "w1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("concurrent upsert: no fact row found")
	}
	var got int64
	if err := rows.Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 50 {
		t.Fatalf("concurrent upsert: fact version=%d want 50", got)
	}
}

func BenchmarkPgAnalytics_UpsertBatch128(b *testing.B) {
	ctx := context.Background()
	s, err := pganalytics.Open(ctx, fabriqtest.StartPostgres(b))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	batch := make([]analytics.Fact, 128)
	for i := range batch {
		batch[i] = analytics.Fact{TenantID: "t1", Aggregate: "widget", AggID: "w" + strconv.Itoa(i), Version: 1, Payload: []byte(`{"n":1}`), At: time.Now()}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range batch {
			batch[j].Version = int64(i + 1)
		}
		if err := s.UpsertFacts(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}
