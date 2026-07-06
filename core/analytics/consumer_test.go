package analytics_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// fakeSource replays a fixed slice of envelopes then returns.
type fakeSource struct{ envs []event.Envelope }

func (f *fakeSource) EnsureGroup(context.Context, string) error { return nil }
func (f *fakeSource) Consume(ctx context.Context, _, _ string, handle func(string, event.Envelope) error) error {
	for i, e := range f.envs {
		if err := handle(itoaTest(i), e); err != nil {
			return err
		}
	}
	return nil
}
func itoaTest(i int) string { return string(rune('a' + i)) }

func TestConsumer_AppliesMarkedEntity(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	c := &analytics.Consumer{
		Group:   "proj:analytics",
		Source:  &fakeSource{envs: []event.Envelope{env("widget", "widget.updated", 1, `{"name":"a","ssn":"x"}`)}},
		Applier: analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})),
		Sink:    sink,
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(sink.Facts()) != 1 || len(sink.Events()) != 1 {
		t.Fatalf("expected 1 fact + 1 event, got %d/%d", len(sink.Facts()), len(sink.Events()))
	}
}

func TestConsumer_SkipsUnmarked(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	c := &analytics.Consumer{
		Group:   "proj:analytics",
		Source:  &fakeSource{envs: []event.Envelope{env("widget", "widget.updated", 1, `{}`)}},
		Applier: analytics.NewApplier(regWith(nil)),
		Sink:    sink,
	}
	_ = c.Run(context.Background(), "c1")
	if len(sink.Facts()) != 0 {
		t.Fatal("unmarked entity should produce no facts")
	}
}

func TestConsumer_ReplayIdempotent(t *testing.T) {
	sink := fabriqtest.NewFakeAnalyticsSink()
	src := &fakeSource{envs: []event.Envelope{
		env("widget", "widget.updated", 2, `{"name":"a"}`),
		env("widget", "widget.updated", 2, `{"name":"a"}`), // redelivery
	}}
	c := &analytics.Consumer{Group: "proj:analytics", Source: src,
		Applier: analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})), Sink: sink}
	_ = c.Run(context.Background(), "c1")
	if sink.Facts()["t1|widget|w1"].Version != 2 {
		t.Fatal("replay should be idempotent at version 2")
	}
}
