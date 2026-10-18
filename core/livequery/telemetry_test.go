package livequery_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	"github.com/xraph/fabriq/core/tenant"
)

// spanProbeFeed records whether the shared feed ctx carries a span.
type spanProbeFeed struct {
	fakeFeed
	feedSpan bool
	tenantID string
}

func (f *spanProbeFeed) Changes(ctx context.Context, q livequery.LiveQuery, from string) (<-chan livequery.Change, func(), error) {
	f.feedSpan = trace.SpanContextFromContext(ctx).IsValid()
	f.tenantID, _ = tenant.FromContext(ctx)
	return f.fakeFeed.Changes(ctx, q, from)
}

// The partition feed outlives the subscriber that created it, so it must
// not keep that subscriber's request span (later refills would land in an
// ended trace), but it keeps the tenant.
func TestPartitionFeedDropsTheFirstSubscribersSpan(t *testing.T) {
	_ = telemetrytest.Record(t)
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &spanProbeFeed{fakeFeed: fakeFeed{ch: make(chan livequery.Change, 1)}}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed, livequery.EngineOptions{Cushion: 2, Buffer: 4})

	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := telemetry.Tracer().Start(ctx, "GET /live")
	defer span.End()
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 2, Where: query.Where{query.Eq("status", "active")}}
	_, _, h, err := eng.Subscribe(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if feed.feedSpan {
		t.Fatal("the shared feed ctx kept the subscriber's request span")
	}
	if feed.tenantID != "acme" {
		t.Fatalf("the feed ctx lost the tenant: %q", feed.tenantID)
	}
}
