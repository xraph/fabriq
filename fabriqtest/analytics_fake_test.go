package fabriqtest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/fabriqtest"
)

var ctx = context.Background()

func TestFakeAnalyticsSink_VersionGate(t *testing.T) {
	s := fabriqtest.NewFakeAnalyticsSink()
	f := func(v int64) analytics.Fact {
		return analytics.Fact{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: v, Payload: json.RawMessage(`{}`)}
	}
	mustUpsert(t, s, f(2))
	mustUpsert(t, s, f(1)) // stale, ignored
	if got := s.Facts()["t1|widget|w1"].Version; got != 2 {
		t.Fatalf("version gate failed: got %d want 2", got)
	}
}

func TestFakeAnalyticsSink_AppendDedupes(t *testing.T) {
	s := fabriqtest.NewFakeAnalyticsSink()
	e := analytics.Event{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 1, Type: "widget.created", Payload: json.RawMessage(`{}`)}
	_ = s.AppendEvents(ctx, []analytics.Event{e})
	_ = s.AppendEvents(ctx, []analytics.Event{e}) // duplicate
	if n := len(s.Events()); n != 1 {
		t.Fatalf("append dedupe failed: got %d want 1", n)
	}
}

func mustUpsert(t *testing.T, s analytics.Sink, f analytics.Fact) {
	t.Helper()
	if err := s.UpsertFacts(ctx, []analytics.Fact{f}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
}
