package analytics_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

type obsFunc func(projection, result string, d time.Duration)

func (f obsFunc) ObserveApply(p, r string, d time.Duration) { f(p, r, d) }

func TestConsumerAppliesUnderALinkedConsumerRoot(t *testing.T) {
	rec := telemetrytest.Record(t)
	e := env("widget.updated", 1, `{"name":"alice@example.com","ssn":"x"}`)
	e.Traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got [][2]string
	c := &analytics.Consumer{
		Group:    "proj:analytics",
		Source:   &fakeSource{envs: []event.Envelope{e, env("widget.updated", 2, `{}`)}},
		Applier:  analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})),
		Sink:     fabriqtest.NewFakeAnalyticsSink(),
		Observer: obsFunc(func(p, r string, _ time.Duration) { got = append(got, [2]string{p, r}) }),
	}
	boot, span := telemetry.Tracer().Start(context.Background(), "start fabriq")
	if err := c.Run(boot, "c1"); err != nil {
		t.Fatal(err)
	}
	span.End()

	spans := telemetrytest.Named(rec, "apply projection event")
	if len(spans) != 2 {
		t.Fatalf("got %d apply spans, want 2", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindConsumer || s.Parent().IsValid() || len(s.Links()) != 1 {
		t.Fatalf("want a linked root consumer span: kind=%v parent=%v links=%d", s.SpanKind(), s.Parent(), len(s.Links()))
	}
	if p, _ := telemetrytest.Attr(s, "fabriq.projection"); p != "analytics" {
		t.Fatalf("fabriq.projection = %q", p)
	}
	if len(got) != 2 || got[0] != [2]string{"analytics", "ok"} {
		t.Fatalf("observer got %v", got)
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "t1")
}

func TestConsumerFailureRecordsError(t *testing.T) {
	rec := telemetrytest.Record(t)
	var result string
	c := &analytics.Consumer{
		Group:    "proj:analytics",
		Source:   &fakeSource{envs: []event.Envelope{env("widget.updated", 1, `{"name":"a"}`)}},
		Applier:  analytics.NewApplier(regWith(&registry.AnalyticsSpec{Include: []string{"name"}})),
		Sink:     failingUpsertSink{fabriqtest.NewFakeAnalyticsSink()},
		Observer: obsFunc(func(_, r string, _ time.Duration) { result = r }),
	}
	_ = c.Run(context.Background(), "c1")
	if result != "error" {
		t.Fatalf("result = %q, want error", result)
	}
	s := telemetrytest.Named(rec, "apply projection event")[0]
	if s.Status().Description != "apply failed" {
		t.Fatalf("status = %+v", s.Status())
	}
	telemetrytest.AssertNoValue(t, rec, "boom")
}
