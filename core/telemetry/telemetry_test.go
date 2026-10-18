package telemetry_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	"github.com/xraph/fabriq/core/tenant"
)

const producerTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestDetachKeepsValuesAndCancellationButDropsSpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	ctx, span := telemetry.Tracer().Start(ctx, "boot")
	defer span.End()

	d := telemetry.Detach(ctx)
	if trace.SpanContextFromContext(d).IsValid() {
		t.Fatal("detached ctx still carries a valid span context")
	}
	if tid, _ := tenant.Require(d); tid != "acme" {
		t.Fatalf("tenant lost: %q", tid)
	}
	cancel()
	if d.Err() == nil {
		t.Fatal("detached ctx must keep the parent's cancellation")
	}
	_ = rec
}

func TestStartConsumerIsALinkedRoot(t *testing.T) {
	rec := telemetrytest.Record(t)
	parent, pspan := telemetry.Tracer().Start(context.Background(), "request")
	_, span := telemetry.StartConsumer(parent, "apply projection event", producerTP,
		telemetry.AttrProjection.String("graph"))
	span.End()
	pspan.End()

	spans := telemetrytest.Named(rec, "apply projection event")
	if len(spans) != 1 {
		t.Fatalf("got %d consumer spans", len(spans))
	}
	s := spans[0]
	if s.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("kind = %v, want consumer", s.SpanKind())
	}
	if s.Parent().IsValid() {
		t.Fatalf("consumer span must be a root, parent = %v", s.Parent())
	}
	if s.SpanContext().TraceID() == pspan.SpanContext().TraceID() {
		t.Fatal("consumer span joined the caller's trace")
	}
	links := s.Links()
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" ||
		links[0].SpanContext.SpanID().String() != "00f067aa0ba902b7" {
		t.Fatalf("links = %+v, want one link to the producer", links)
	}
}

func TestStartConsumerWithoutTraceparentHasNoLink(t *testing.T) {
	rec := telemetrytest.Record(t)
	for _, tp := range []string{"", "garbage"} {
		_, span := telemetry.StartConsumer(context.Background(), "apply projection event", tp)
		span.End()
	}
	for _, s := range rec.Ended() {
		if len(s.Links()) != 0 {
			t.Fatalf("unexpected links %+v", s.Links())
		}
	}
}

func TestFailRecordsTypeNeverText(t *testing.T) {
	rec := telemetrytest.Record(t)
	_, span := telemetry.Tracer().Start(context.Background(), "op")
	telemetry.Fail(span, "write failed", fmt.Errorf("insert %q: %w", "alice@example.com", errors.New("boom")))
	span.End()

	s := rec.Ended()[0]
	if s.Status().Code != codes.Error || s.Status().Description != "write failed" {
		t.Fatalf("status = %+v", s.Status())
	}
	if len(s.Events()) != 1 || s.Events()[0].Name != "exception" {
		t.Fatalf("events = %+v", s.Events())
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "boom")
}

func TestErrorTypeAndClassify(t *testing.T) {
	conflict := fmt.Errorf("wrap: %w", &fabriqerr.VersionConflictError{Expected: 1, Actual: 2})
	if got := telemetry.ErrorType(conflict); got != "fabriqerr.ErrVersionConflict" {
		t.Fatalf("ErrorType(conflict) = %q", got)
	}
	if got := telemetry.ErrorType(fmt.Errorf("x: %w", tenant.ErrNoTenant)); got != "tenant.ErrNoTenant" {
		t.Fatalf("ErrorType(no tenant) = %q", got)
	}
	type custom struct{ error }
	if got := telemetry.ErrorType(fmt.Errorf("x: %w", custom{errors.New("y")})); got != "telemetry_test.custom" {
		t.Fatalf("ErrorType(custom) = %q", got)
	}
	cases := map[string]error{
		telemetry.ResultOK:       nil,
		telemetry.ResultConflict: conflict,
		telemetry.ResultNotFound: fmt.Errorf("x: %w", fabriqerr.ErrNotFound),
		telemetry.ResultError:    errors.New("down"),
	}
	for want, err := range cases {
		if got := telemetry.Classify(err); got != want {
			t.Fatalf("Classify(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestWithoutBaggage(t *testing.T) {
	m, err := baggage.NewMember("user", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	b, err := baggage.New(m)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), b)
	if got := baggage.FromContext(telemetry.WithoutBaggage(ctx)); got.Len() != 0 {
		t.Fatalf("baggage survived: %v", got)
	}
}
