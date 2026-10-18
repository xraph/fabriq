// Package tracing gives the root module its own instrumentation scope.
// Spans the root starts (the relay, the worker's background units, admin
// jobs) carry github.com/xraph/fabriq; spans core starts carry
// github.com/xraph/fabriq/core. Both use the global tracer provider forge.New
// installs. The span helpers (Detach, Fail, links) come from core/telemetry.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
)

// ScopeName is the instrumentation scope of every span the root starts.
const ScopeName = "github.com/xraph/fabriq"

// Tracer returns the root's tracer from the global provider, looked up per
// call so a provider installed later (forge.New, a test) is the one used.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// StartRoot starts a new root span for one unit of background work, from a
// copy of ctx with any inherited span removed.
func StartRoot(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts = append([]trace.SpanStartOption{trace.WithNewRoot()}, opts...)
	return Tracer().Start(telemetry.Detach(ctx), name, opts...)
}
