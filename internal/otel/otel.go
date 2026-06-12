// Package otel carries W3C trace context across fabriq's async hop: the
// command executor stamps the active traceparent into the event envelope,
// and consumers (relay, projections) restore it so ONE trace spans
// command -> outbox -> relay -> projection apply.
//
// Wire it as fabriq.WithTraceparent(otel.TraceparentFromContext).
package otel

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var propagator = propagation.TraceContext{}

// TraceparentFromContext renders the active span as a W3C traceparent
// header value, or "" when no span is recording.
func TraceparentFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextWithTraceparent restores a traceparent (from an event envelope)
// onto a context, so consumer-side spans join the original trace. Invalid
// values are ignored.
func ContextWithTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
