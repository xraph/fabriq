//go:build duckdb

package duckanalytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/duckanalytics"
	"github.com/xraph/fabriq/core/analytics"
)

func TestDuckAnalytics_QueryReadOnly(t *testing.T) {
	ctx := context.Background()
	s, err := duckanalytics.Open(ctx, "duckdb://:memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	facts := []analytics.Fact{
		{TenantID: "t1", Aggregate: "order", AggID: "o1", Version: 1, Payload: []byte(`{"n":1}`), At: time.Now()},
		{TenantID: "t1", Aggregate: "order", AggID: "o2", Version: 1, Payload: []byte(`{"n":2}`), At: time.Now()},
		{TenantID: "t1", Aggregate: "customer", AggID: "c1", Version: 1, Payload: []byte(`{}`), At: time.Now()},
	}
	if err := s.UpsertFacts(ctx, facts); err != nil {
		t.Fatal(err)
	}

	rows, cols, truncated, err := s.QueryReadOnly(ctx,
		`SELECT aggregate, count(*) AS n FROM fabriq_analytics_facts WHERE tenant_id = ? GROUP BY aggregate ORDER BY aggregate`, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if truncated {
		t.Fatalf("unexpected truncation")
	}
	if len(cols) != 2 || cols[0] != "aggregate" {
		t.Fatalf("cols = %v, want [aggregate n]", cols)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (customer, order)", len(rows))
	}

	// Truncation: shrink the cap and verify it trips.
	duckanalytics.SetMaxAnalyticsQueryRowsForTest(1)
	defer duckanalytics.SetMaxAnalyticsQueryRowsForTest(1000)
	rows, _, truncated, err = s.QueryReadOnly(ctx, `SELECT * FROM fabriq_analytics_facts WHERE tenant_id = ?`, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || len(rows) != 1 {
		t.Fatalf("truncated=%v rows=%d, want truncated=true rows=1", truncated, len(rows))
	}
}
