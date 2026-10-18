package agent

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

type baggageProbe struct {
	members int
	parent  trace.SpanContext
	err     error
}

func (b *baggageProbe) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	b.members = baggage.FromContext(ctx).Len()
	b.parent = trace.SpanContextFromContext(ctx)
	return make([][]float32, len(texts)), b.err
}
func (b *baggageProbe) Dims() int { return 3 }

func (b *baggageProbe) Summarize(ctx context.Context, _ SummaryInput) (string, error) {
	b.members = baggage.FromContext(ctx).Len()
	b.parent = trace.SpanContextFromContext(ctx)
	return "ok", b.err
}

func withClientBaggage(t *testing.T) context.Context {
	t.Helper()
	m, err := baggage.NewMember("user", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	b, err := baggage.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return baggage.ContextWithBaggage(context.Background(), b)
}

// A model provider sits behind the Embedder and Summarizer, so the call must
// not carry the client's inbound baggage, and it runs under its own span.
func TestModelCallsDropBaggageAndRunUnderASpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	ctx, req := telemetry.Tracer().Start(withClientBaggage(t), "request")

	emb := &baggageProbe{}
	if _, err := embedTexts(ctx, emb, []string{"alice@example.com", "b"}); err != nil {
		t.Fatal(err)
	}
	if emb.members != 0 {
		t.Fatalf("embedder saw %d baggage members", emb.members)
	}
	sum := &baggageProbe{err: errors.New("provider said alice@example.com is rate limited")}
	if _, err := summarizeText(ctx, sum, SummaryInput{Raw: []byte("alice@example.com")}); err == nil {
		t.Fatal("want the summarizer's error back")
	}
	if sum.members != 0 {
		t.Fatalf("summarizer saw %d baggage members", sum.members)
	}
	req.End()

	e := telemetrytest.Named(rec, "call embedder")
	s := telemetrytest.Named(rec, "call summarizer")
	if len(e) != 1 || len(s) != 1 {
		t.Fatalf("spans: embedder=%d summarizer=%d", len(e), len(s))
	}
	if e[0].Parent().SpanID() != req.SpanContext().SpanID() || emb.parent.SpanID() != e[0].SpanContext().SpanID() {
		t.Fatal("call embedder must be a child of the caller and parent of the provider call")
	}
	if n, _ := telemetrytest.Attr(e[0], "fabriq.model.inputs"); n != "2" {
		t.Fatalf("fabriq.model.inputs = %q", n)
	}
	if s[0].Status().Description != "summarize failed" {
		t.Fatalf("status = %+v", s[0].Status())
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
}
