package shard_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/query"
)

// analyticsStub implements query.AnalyticsQuerier, recording into its
// parent's log. A separate type (rather than adding methods to stub) keeps
// this test isolated from the other source-of-truth stubs in shard_test.go.
type analyticsStub struct{ parent *stub }

func (a analyticsStub) Track(_ context.Context, events []query.AnalyticsEvent) error {
	a.parent.calls = append(a.parent.calls, fmt.Sprintf("Track:%d", len(events)))
	return nil
}
func (a analyticsStub) Query(_ context.Context, q query.AnalyticsQuery, _ any) error {
	a.parent.calls = append(a.parent.calls, "AnalyticsQuery:"+q.Source)
	return nil
}
func (a analyticsStub) QueryRaw(_ context.Context, _ any, sql string, _ ...any) error {
	a.parent.calls = append(a.parent.calls, "QueryRaw:"+sql)
	return nil
}

// shardWithAnalytics builds a Shard like shardFor, plus an Analytics stub
// routed to the same call log.
func shardWithAnalytics(s *stub) shard.Shard {
	sh := shardFor(s)
	sh.Analytics = analyticsStub{parent: s}
	return sh
}

func TestInsights_NewAnalytics_RoutesByTenant(t *testing.T) {
	s0, s1 := &stub{id: "0"}, &stub{id: "1"}
	set, err := shard.New(mapDir{"acme": "0", "globex": "1"}, shardWithAnalytics(s0), shardWithAnalytics(s1))
	if err != nil {
		t.Fatal(err)
	}

	an := shard.NewAnalytics(set)
	if an == nil {
		t.Fatal("NewAnalytics returned nil")
	}

	// acme -> shard 0
	if err := an.Track(ctxFor(t, "acme"), []query.AnalyticsEvent{{Name: "signup"}}); err != nil {
		t.Fatal(err)
	}
	// globex -> shard 1
	if err := an.Query(ctxFor(t, "globex"), query.AnalyticsQuery{Source: "signup"}, nil); err != nil {
		t.Fatal(err)
	}

	if len(s0.calls) != 1 || s0.calls[0] != "Track:1" {
		t.Fatalf("shard 0 calls = %v", s0.calls)
	}
	if len(s1.calls) != 1 || s1.calls[0] != "AnalyticsQuery:signup" {
		t.Fatalf("shard 1 calls = %v", s1.calls)
	}
}

func TestInsights_NewAnalytics_QueryRaw(t *testing.T) {
	s := &stub{id: "0"}
	set := shard.Single(shardWithAnalytics(s))
	an := shard.NewAnalytics(set)

	if err := an.QueryRaw(ctxFor(t, "acme"), nil, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if len(s.calls) != 1 || s.calls[0] != "QueryRaw:SELECT 1" {
		t.Fatalf("calls = %v", s.calls)
	}
}
