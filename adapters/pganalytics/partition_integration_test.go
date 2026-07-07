//go:build integration

package pganalytics_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestPgAnalytics_PartitionedConformance runs the full sink contract against a
// range-partitioned event log — proving partitioning does not change any
// observable Sink behavior (dedup, prune, reproject, etc.).
func TestPgAnalytics_PartitionedConformance(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)
	analytics.RunSinkConformance(t, func() analytics.Sink {
		s, err := pganalytics.Open(context.Background(), dsn, pganalytics.WithEventPartitioning())
		if err != nil {
			t.Fatal(err)
		}
		_ = pganalytics.TruncateForTest(context.Background(), s)
		return s
	})
}

// TestPgAnalytics_PartitionRoutingAndDrop proves the partitioned event log:
// the table is partitioned with a default catch-all, current-month writes land
// in a month partition while a historical write lands in default, dedup still
// holds, and MaintainPartitions drops a fully-aged month partition.
func TestPgAnalytics_PartitionRoutingAndDrop(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	s, err := pganalytics.Open(ctx, dsn, pganalytics.WithEventPartitioning())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// The events table is partitioned.
	if got := fabriqtest.QueryStrings(t, dsn,
		`SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid
		 WHERE c.relname = 'fabriq_analytics_events'`); len(got) != 1 {
		t.Fatal("fabriq_analytics_events is not partitioned")
	}

	ev := func(id string, at time.Time) analytics.Event {
		return analytics.Event{TenantID: "t1", Aggregate: "widget", AggID: id, Version: 1,
			Type: "widget.created", Payload: json.RawMessage(`{}`), At: at}
	}
	now := time.Now().UTC()
	old := now.AddDate(-2, 0, 0) // two years ago → default partition

	mustAppend(t, s, ev("cur", now))
	mustAppend(t, s, ev("cur", now)) // dedup: same (tenant,agg,id,version,at)
	mustAppend(t, s, ev("hist", old))

	// Dedup held: exactly one "cur" row.
	if got := fabriqtest.QueryStrings(t, dsn,
		`SELECT count(*)::text FROM fabriq_analytics_events WHERE agg_id='cur'`); got[0] != "1" {
		t.Fatalf("cur row count = %s, want 1 (dedup with at in the key)", got[0])
	}
	// Historical write landed in the default partition (no ancient month partition).
	if got := fabriqtest.QueryStrings(t, dsn,
		`SELECT count(*)::text FROM fabriq_analytics_events_default WHERE agg_id='hist'`); got[0] != "1" {
		t.Fatalf("historical event not in default partition (got %s)", got[0])
	}

	// Create an old month partition with a row, then drop everything older than 1 day.
	oldMonth := now.AddDate(0, -3, 0)
	pname := oldMonthPartition(oldMonth)
	fabriqtest.QueryStrings(t, dsn,
		`CREATE TABLE `+pname+` PARTITION OF fabriq_analytics_events FOR VALUES FROM ('`+
			monthFloor(oldMonth).Format("2006-01-02")+`') TO ('`+
			monthFloor(oldMonth).AddDate(0, 1, 0).Format("2006-01-02")+`')`)
	mustAppend(t, s, ev("aged", oldMonth))

	// (current/next partitions were already created at Open, so this call only
	// drops.) Assert the aged month partition is reclaimed.
	_, dropped, err := s.MaintainPartitions(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if dropped < 1 {
		t.Fatalf("dropped = %d, want the aged month partition dropped", dropped)
	}
	// The aged row is gone (its whole partition was dropped).
	if got := fabriqtest.QueryStrings(t, dsn,
		`SELECT count(*)::text FROM fabriq_analytics_events WHERE agg_id='aged'`); got[0] != "0" {
		t.Fatalf("aged event survived partition drop (got %s)", got[0])
	}
}

func mustAppend(t *testing.T, s *pganalytics.Sink, e analytics.Event) {
	t.Helper()
	if err := s.AppendEvents(context.Background(), []analytics.Event{e}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func monthFloor(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

func oldMonthPartition(t time.Time) string {
	f := monthFloor(t)
	return "fabriq_analytics_events_" + f.Format("200601")
}
