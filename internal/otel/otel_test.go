package otel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestTraceparentFromContext_NoSpan(t *testing.T) {
	if got := TraceparentFromContext(context.Background()); got != "" {
		t.Fatalf("no span: traceparent = %q, want empty", got)
	}
}

func TestTraceparentFromContext_WithSpan(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	want := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if got := TraceparentFromContext(ctx); got != want {
		t.Fatalf("traceparent = %q, want %q", got, want)
	}
}

func TestContextWithTraceparent_RoundTrip(t *testing.T) {
	raw := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := ContextWithTraceparent(context.Background(), raw)
	if got := TraceparentFromContext(ctx); got != raw {
		t.Fatalf("round trip = %q, want %q", got, raw)
	}
}

func TestContextWithTraceparent_GarbageIgnored(t *testing.T) {
	ctx := ContextWithTraceparent(context.Background(), "not-a-traceparent")
	if got := TraceparentFromContext(ctx); got != "" {
		t.Fatalf("garbage produced traceparent %q", got)
	}
}
