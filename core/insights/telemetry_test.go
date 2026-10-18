package insights_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

type obsFunc func(projection, result string, d time.Duration)

func (f obsFunc) ObserveApply(p, r string, d time.Duration) { f(p, r, d) }

func TestConsumerAppliesUnderALinkedConsumerRoot(t *testing.T) {
	rec := telemetrytest.Record(t)
	e := env("widget.updated", 1, `{"name":"alice@example.com","price":10}`)
	e.Traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got [][2]string
	c := &insights.Consumer{
		Group:    "proj:insights",
		Source:   &fakeSource{envs: []event.Envelope{e}},
		Applier:  insights.NewApplier(regWith(&registry.InsightsSpec{Measures: []string{"price"}, Dimensions: []string{"name"}})),
		Sink:     &fakeSink{},
		Observer: obsFunc(func(p, r string, _ time.Duration) { got = append(got, [2]string{p, r}) }),
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	spans := telemetrytest.Named(rec, "apply projection event")
	if len(spans) != 1 {
		t.Fatalf("got %d apply spans", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindConsumer || s.Parent().IsValid() || len(s.Links()) != 1 {
		t.Fatalf("want a linked root consumer span: kind=%v parent=%v links=%d", s.SpanKind(), s.Parent(), len(s.Links()))
	}
	if p, _ := telemetrytest.Attr(s, "fabriq.projection"); p != "insights" {
		t.Fatalf("fabriq.projection = %q", p)
	}
	if len(got) != 1 || got[0] != [2]string{"insights", "ok"} {
		t.Fatalf("observer got %v", got)
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
}
