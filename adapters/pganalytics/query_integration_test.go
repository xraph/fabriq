//go:build integration

package pganalytics_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestPgAnalytics_QueryReadOnly(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	s, err := pganalytics.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.UpsertFacts(ctx, []analytics.Fact{
		{TenantID: "t1", Aggregate: "order", AggID: "o1", Version: 1, Payload: []byte(`{"n":1}`), At: time.Now()},
		{TenantID: "t1", Aggregate: "order", AggID: "o2", Version: 1, Payload: []byte(`{"n":2}`), At: time.Now()},
	}); err != nil {
		t.Fatal(err)
	}

	rows, cols, _, err := s.QueryReadOnly(ctx,
		`SELECT aggregate, count(*) AS n FROM fabriq_analytics_facts WHERE tenant_id = $1 GROUP BY aggregate`, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Fatalf("cols = %v", cols)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}

	// The READ ONLY tx is the real enforcement: a write must fail at the DB
	// even though it bypasses the adminapi precheck here.
	if _, _, _, werr := s.QueryReadOnly(ctx, `DELETE FROM fabriq_analytics_facts WHERE tenant_id = $1`, "t1"); werr == nil {
		t.Fatalf("write inside read-only tx unexpectedly succeeded")
	}
}
