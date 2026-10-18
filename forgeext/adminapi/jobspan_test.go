package adminapi

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

func TestRunJobTracedIsALinkedRoot(t *testing.T) {
	rec := telemetrytest.Record(t)
	origin, req := telemetry.Tracer().Start(context.Background(), "POST /analytics/backfill")
	req.End() // the request is long gone by the time the job runs

	var jobCtxSpan trace.SpanContext
	_, err := runJobTraced(origin, "backfill", func(ctx context.Context) (any, error) {
		jobCtxSpan = trace.SpanContextFromContext(ctx)
		return nil, errors.New("tenant acme: connection refused")
	})
	if err == nil {
		t.Fatal("the job's error must come back")
	}

	s := telemetrytest.Named(rec, "run admin job")
	if len(s) != 1 {
		t.Fatalf("got %d job spans", len(s))
	}
	if s[0].Parent().IsValid() || s[0].SpanContext().TraceID() == req.SpanContext().TraceID() {
		t.Fatal("the job span must be a new root, not part of the request trace")
	}
	if l := s[0].Links(); len(l) != 1 || l[0].SpanContext.SpanID() != req.SpanContext().SpanID() {
		t.Fatalf("links = %+v, want one to the request", l)
	}
	if jobCtxSpan.SpanID() != s[0].SpanContext().SpanID() {
		t.Fatal("the job must run under its span")
	}
	if k, _ := telemetrytest.Attr(s[0], "fabriq.job.kind"); k != "backfill" {
		t.Fatalf("fabriq.job.kind = %q", k)
	}
	if s[0].Status().Code != codes.Error || s[0].Status().Description != "job failed" {
		t.Fatalf("status = %+v", s[0].Status())
	}
	telemetrytest.AssertNoValue(t, rec, "acme")
	telemetrytest.AssertNoValue(t, rec, "connection refused")
}
