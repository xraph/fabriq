package shard_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
)

// analyticsStub implements query.AnalyticsQuerier AND insights.FactSink,
// recording into its parent's log. A separate type (rather than adding
// methods to stub) keeps this test isolated from the other source-of-truth
// stubs in shard_test.go.
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
func (a analyticsStub) UpsertInsightFacts(_ context.Context, facts []insights.Fact) error {
	a.parent.calls = append(a.parent.calls, fmt.Sprintf("UpsertInsightFacts:%d", len(facts)))
	return nil
}

var _ insights.FactSink = analyticsStub{}

// analyticsQuerierOnlyStub implements query.AnalyticsQuerier but deliberately
// NOT insights.FactSink, modeling a shard whose Analytics adapter predates
// the FactSink passthrough (or a test double that never wired it).
type analyticsQuerierOnlyStub struct{ parent *stub }

func (a analyticsQuerierOnlyStub) Track(context.Context, []query.AnalyticsEvent) error { return nil }
func (a analyticsQuerierOnlyStub) Query(context.Context, query.AnalyticsQuery, any) error {
	return nil
}
func (a analyticsQuerierOnlyStub) QueryRaw(context.Context, any, string, ...any) error { return nil }

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

// TestInsights_NewAnalytics_UpsertInsightFacts_RoutesByTenant is the crux of
// Task 11's per-tenant FactSink resolution: shard.NewAnalytics(set), used as
// the proj:insights consumer's Sink, must resolve the SAME shard the ctx
// tenant already routes to for Track/Query/QueryRaw — Acquire derives the
// tenant from ctx (the consumer's handle stamps it there before calling the
// sink), not from an explicit argument.
func TestInsights_NewAnalytics_UpsertInsightFacts_RoutesByTenant(t *testing.T) {
	s0, s1 := &stub{id: "0"}, &stub{id: "1"}
	set, err := shard.New(mapDir{"acme": "0", "globex": "1"}, shardWithAnalytics(s0), shardWithAnalytics(s1))
	if err != nil {
		t.Fatal(err)
	}

	sink := shard.NewAnalytics(set)
	var _ insights.FactSink = sink

	facts := []insights.Fact{{TenantID: "acme", Entity: "order", AggID: "o1", Version: 1}}
	if err := sink.UpsertInsightFacts(ctxFor(t, "acme"), facts); err != nil {
		t.Fatal(err)
	}
	if err := sink.UpsertInsightFacts(ctxFor(t, "globex"), nil); err != nil {
		t.Fatal(err)
	}

	if len(s0.calls) != 1 || s0.calls[0] != "UpsertInsightFacts:1" {
		t.Fatalf("shard 0 (acme) calls = %v", s0.calls)
	}
	if len(s1.calls) != 1 || s1.calls[0] != "UpsertInsightFacts:0" {
		t.Fatalf("shard 1 (globex) calls = %v", s1.calls)
	}
}

// TestInsights_NewAnalytics_UpsertInsightFacts_NotFactSink covers the
// defensive branch: a shard whose Analytics adapter implements
// query.AnalyticsQuerier but not insights.FactSink must fail loudly rather
// than silently drop facts.
func TestInsights_NewAnalytics_UpsertInsightFacts_NotFactSink(t *testing.T) {
	s := &stub{id: "0"}
	sh := shardFor(s)
	sh.Analytics = analyticsQuerierOnlyStub{parent: s}
	set := shard.Single(sh)
	sink := shard.NewAnalytics(set)

	err := sink.UpsertInsightFacts(ctxFor(t, "acme"), []insights.Fact{{TenantID: "acme"}})
	if err == nil {
		t.Fatal("expected an error when the shard's Analytics adapter does not implement insights.FactSink")
	}
}
