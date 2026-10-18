// Package otel carries W3C trace context across fabriq's async hop: the
// command executor stamps the active traceparent into the event envelope
// and the outbox row. The relay and the projection consumers start their own
// root spans and LINK them to that stored context (core/telemetry), because
// the request that wrote the command has usually ended by the time its
// event is published or applied.
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
// onto a context as its remote parent. Invalid values are ignored. fabriq's
// own consumers no longer use it: they link to the stored context with
// telemetry.StartConsumer instead of parenting on it.
func ContextWithTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}
