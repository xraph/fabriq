package adminapi

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/internal/tracing"
)

// runJobTraced runs one detached admin job under its own root span, "run
// admin job", linked to the request that started it (origin). The job
// outlives that request, so it gets its own trace. kind is a fixed job name
// (backfill, up, provision, ...); tenant IDs never reach the span. The job's
// ctx starts from context.Background, as the jobs always have, so no request
// value or baggage rides along.
func runJobTraced[T any](origin context.Context, kind string, run func(context.Context) (T, error)) (T, error) {
	opts := []trace.SpanStartOption{trace.WithAttributes(attribute.String("fabriq.job.kind", kind))}
	if sc := trace.SpanContextFromContext(origin); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	ctx, span := tracing.StartRoot(context.Background(), "run admin job", opts...)
	defer span.End()
	res, err := run(ctx)
	if err != nil {
		telemetry.Fail(span, "job failed", err)
	}
	return res, err
}
