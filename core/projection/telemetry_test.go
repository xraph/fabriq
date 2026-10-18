package projection_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

const producerTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// spanProbeSource records whether the loop's own calls (EnsureGroup,
// Consume) see a valid span context, hands each queued envelope to the
// handler once, and returns.
type spanProbeSource struct {
	queue     []event.Envelope
	loopSpans []bool
	errs      []error
}

func (s *spanProbeSource) EnsureGroup(ctx context.Context, _ string) error {
	s.loopSpans = append(s.loopSpans, trace.SpanContextFromContext(ctx).IsValid())
	return nil
}

func (s *spanProbeSource) Consume(ctx context.Context, _, _ string, handle func(string, event.Envelope) error) error {
	s.loopSpans = append(s.loopSpans, trace.SpanContextFromContext(ctx).IsValid())
	for i, env := range s.queue {
		s.errs = append(s.errs, handle(streamID(i), env))
	}
	return nil
}

// parentProbeSink records the span each apply runs under.
type parentProbeSink struct {
	inner projection.Sink
	fail  bool
	seen  []trace.SpanContext
}

func (p *parentProbeSink) ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error {
	p.seen = append(p.seen, trace.SpanContextFromContext(ctx))
	if p.fail {
		return errors.New("falkor down: alice@example.com")
	}
	return p.inner.ApplyMutations(ctx, target, muts)
}

type applyObs struct {
	mu  sync.Mutex
	got [][2]string
}

func (o *applyObs) ObserveApply(projection, result string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.got = append(o.got, [2]string{projection, result})
}

func TestEngineAppliesUnderALinkedConsumerRoot(t *testing.T) {
	rec := telemetrytest.Record(t)
	w, reg := engineWorld(t)
	env := assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "name": "alice@example.com"})
	env.Traceparent = producerTP
	src := &spanProbeSource{queue: []event.Envelope{env}}
	sink := &parentProbeSink{inner: w.Graph}
	obs := &applyObs{}
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: sink,
		Applier:  projection.GraphApplier(reg),
		State:    w.Projections,
		Observer: obs,
	}

	// Start the loop under a span, as forge's boot does.
	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	if err := e.Run(boot, "c1"); err != nil {
		t.Fatal(err)
	}
	bootSpan.End()

	for i, valid := range src.loopSpans {
		if valid {
			t.Fatalf("loop call %d ran under the boot span", i)
		}
	}
	spans := telemetrytest.Named(rec, "apply projection event")
	if len(spans) != 1 {
		t.Fatalf("got %d apply spans", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindConsumer || s.Parent().IsValid() {
		t.Fatalf("want a root consumer span, kind=%v parent=%v", s.SpanKind(), s.Parent())
	}
	if s.SpanContext().TraceID() == bootSpan.SpanContext().TraceID() {
		t.Fatal("apply joined the boot trace")
	}
	if l := s.Links(); len(l) != 1 || l[0].SpanContext.SpanID().String() != "00f067aa0ba902b7" {
		t.Fatalf("links = %+v", l)
	}
	for k, want := range map[string]string{
		"fabriq.projection": "graph", "messaging.consumer.group.name": "proj:graph",
		"fabriq.entity": "asset",
	} {
		if got, _ := telemetrytest.Attr(s, k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
	if len(sink.seen) != 1 || sink.seen[0].SpanID() != s.SpanContext().SpanID() {
		t.Fatal("sink writes must run under the apply span")
	}
	if len(obs.got) != 1 || obs.got[0] != [2]string{"graph", "ok"} {
		t.Fatalf("observer got %v", obs.got)
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "acme")
	telemetrytest.AssertNoValue(t, rec, "A1")
}

func TestEngineApplyFailureIsAnErrorWithoutText(t *testing.T) {
	rec := telemetrytest.Record(t)
	w, reg := engineWorld(t)
	src := &spanProbeSource{queue: []event.Envelope{
		assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "name": "Pump"}),
	}}
	obs := &applyObs{}
	e := &projection.Engine{
		Projection: "search", Group: "proj:search",
		Source: src, Sink: &parentProbeSink{inner: w.Graph, fail: true},
		Applier:  projection.GraphApplier(reg),
		State:    w.Projections,
		Observer: obs,
	}
	if err := e.Run(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	if len(src.errs) != 1 || src.errs[0] == nil {
		t.Fatalf("the failure must reach the source for redelivery, got %v", src.errs)
	}
	s := telemetrytest.Named(rec, "apply projection event")[0]
	if s.Status().Code != codes.Error || s.Status().Description != "apply failed" {
		t.Fatalf("status = %+v", s.Status())
	}
	if len(obs.got) != 1 || obs.got[0] != [2]string{"search", "error"} {
		t.Fatalf("observer got %v", obs.got)
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "falkor down")
}

func TestEngineSkipsMintNoSpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	w, reg := engineWorld(t)
	poison := assetCreated(t, map[string]any{"id": "A1"})
	poison.Payload = []byte("{not json")
	noTenant := assetCreated(t, map[string]any{"id": "A2", "version": 1})
	noTenant.TenantID = ""
	src := &spanProbeSource{queue: []event.Envelope{poison, noTenant}}
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: w.Graph,
		Applier: projection.GraphApplier(reg),
		State:   w.Projections,
	}
	if err := e.Run(context.Background(), "c1"); err != nil {
		t.Fatal(err)
	}
	if n := len(telemetrytest.Named(rec, "apply projection event")); n != 0 {
		t.Fatalf("skipped envelopes minted %d spans", n)
	}
}
