// Package telemetry is the small amount of OpenTelemetry glue fabriq's core
// shares with the root module: the tracer, the helpers that keep background
// work off the trace that started it, the producer-to-consumer span link, an
// error marker that never copies error text onto a span, and the observer
// interfaces through which a host records fabriq's duration metrics.
//
// Core records no metrics itself and installs no provider. Spans go to the
// global tracer provider (forge.New installs one); durations go to whatever
// observer the host wires in (fabriq's forge extension records them on its
// Prometheus registry). A nil observer records nothing.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// ScopeName is the instrumentation scope of every span core starts.
const ScopeName = "github.com/xraph/fabriq/core"

// Tracer returns core's tracer from the global provider. It is looked up per
// call so a provider installed after package init (forge.New, a test's
// recorder) is always the one in use.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// Result values shared by fabriq's duration metrics and span attributes.
const (
	ResultOK       = "ok"
	ResultError    = "error"
	ResultInvalid  = "invalid"
	ResultConflict = "conflict"
	ResultNotFound = "not_found"
)

// Attribute keys fabriq puts on spans. Values are counts, booleans and
// bounded enum strings, plus the entity name (spans only, never a metric
// label: runtime dynamic entities make it unbounded).
const (
	AttrEntity     = attribute.Key("fabriq.entity")
	AttrProjection = attribute.Key("fabriq.projection")
	AttrResult     = attribute.Key("fabriq.result")
	AttrBatchSize  = attribute.Key("fabriq.batch_size")
	AttrMutations  = attribute.Key("fabriq.mutations")
	AttrOp         = attribute.Key("fabriq.command.op")
	AttrModelKind  = attribute.Key("fabriq.model.kind")
	AttrInputs     = attribute.Key("fabriq.model.inputs")
	// AttrConsumerGroup is the OTel messaging semconv key for a consumer
	// group (proj:graph, proj:search, ...).
	AttrConsumerGroup = attribute.Key("messaging.consumer.group.name")
	// AttrMessagingSystem is the OTel messaging semconv system key.
	AttrMessagingSystem = attribute.Key("messaging.system")
)

// Detach returns ctx with its span removed. Cancellation, deadlines and
// values (the tenant) survive; only the trace parent goes. Every long-lived
// loop calls it once where its ctx is made, so queries an idle tick runs
// never land in the boot or request trace the loop was started from (grove
// starts a db.client span under any valid span context, even an ended one).
func Detach(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

// WithoutBaggage drops inbound W3C baggage from ctx. Calls that leave for a
// third party (a model provider behind an Embedder or Summarizer) start from
// it, so a client's baggage never travels to that provider.
func WithoutBaggage(ctx context.Context) context.Context {
	return baggage.ContextWithoutBaggage(ctx)
}

var traceContext = propagation.TraceContext{}

// LinkFromTraceparent turns a stored W3C traceparent into a span link. ok is
// false for an empty or malformed value.
func LinkFromTraceparent(traceparent string) (trace.Link, bool) {
	if traceparent == "" {
		return trace.Link{}, false
	}
	ctx := traceContext.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceparent})
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}

// StartRoot starts a new root span on core's tracer from a detached copy of
// ctx, for one unit of background work. opts may add a kind, links and
// attributes.
func StartRoot(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts = append([]trace.SpanStartOption{trace.WithNewRoot()}, opts...)
	return Tracer().Start(Detach(ctx), name, opts...)
}

// StartConsumer starts the span for one consumed message: a new root of kind
// Consumer, linked (not parented) to the producer context stored in
// traceparent. The request that produced the message has usually finished by
// the time it is consumed, so the consumer's work gets its own trace and the
// link leads back to the producer.
func StartConsumer(ctx context.Context, name, traceparent string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...)}
	if link, ok := LinkFromTraceparent(traceparent); ok {
		opts = append(opts, trace.WithLinks(link))
	}
	return StartRoot(ctx, name, opts...)
}

// Fail marks span as failed. The status description is class, a short fixed
// string chosen by the caller, and the exception event carries only the
// error's type. Error text never reaches the span: driver and library
// messages quote values (keys, payload fragments, DSNs).
func Fail(span trace.Span, class string, err error) {
	if err == nil {
		return
	}
	span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", ErrorType(err))))
	span.SetStatus(codes.Error, class)
}

// sentinels maps fabriq's sentinel errors to stable names, so an exception
// event says fabriqerr.ErrVersionConflict and not *errors.errorString.
var sentinels = []struct {
	err  error
	name string
}{
	{fabriqerr.ErrVersionConflict, "fabriqerr.ErrVersionConflict"},
	{fabriqerr.ErrNotFound, "fabriqerr.ErrNotFound"},
	{fabriqerr.ErrStoreNotConfigured, "fabriqerr.ErrStoreNotConfigured"},
	{fabriqerr.ErrQueryTimeout, "fabriqerr.ErrQueryTimeout"},
	{fabriqerr.ErrProjectionLag, "fabriqerr.ErrProjectionLag"},
	{tenant.ErrNoTenant, "tenant.ErrNoTenant"},
	{context.Canceled, "context.Canceled"},
	{context.DeadlineExceeded, "context.DeadlineExceeded"},
}

// ErrorType names err for an exception.type attribute: a known sentinel's
// name, else the Go type of the innermost wrapped error.
func ErrorType(err error) string {
	for _, s := range sentinels {
		if errors.Is(err, s.err) {
			return s.name
		}
	}
	inner := err
	for {
		next := errors.Unwrap(inner)
		if next == nil {
			break
		}
		inner = next
	}
	return fmt.Sprintf("%T", inner)
}

// Classify maps an error onto the bounded result enum the duration metrics
// use: ok, not_found, conflict or error.
func Classify(err error) string {
	switch {
	case err == nil:
		return ResultOK
	case errors.Is(err, fabriqerr.ErrNotFound):
		return ResultNotFound
	case errors.Is(err, fabriqerr.ErrVersionConflict):
		return ResultConflict
	default:
		return ResultError
	}
}

// CommandObserver receives one observation per command batch the executor
// runs. op is create, update, delete, upsert, or mixed for a batch that
// combines several; result is ok, invalid, conflict, not_found or error.
type CommandObserver interface {
	ObserveCommand(op, result string, elapsed time.Duration)
}

// ApplyObserver receives one observation per envelope a projection consumer
// applies. projection is graph, search, analytics or insights; result is ok
// or error.
type ApplyObserver interface {
	ObserveApply(projection, result string, elapsed time.Duration)
}
