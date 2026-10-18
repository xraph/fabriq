//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/otel"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	"github.com/xraph/fabriq/domain"
)

// The relay runs under forge's boot span. Its idle polls must trace nothing,
// and a batch that finds rows is a root Producer span linked to each row's
// writer, never a child of the boot span.
func TestRelay_BatchIsALinkedProducerRoot(t *testing.T) {
	h, r := newRelayHarness(t)
	rec := telemetrytest.Record(t)

	x, err := command.NewExecutor(h.Reg, h.A, command.WithTraceparent(otel.TraceparentFromContext))
	if err != nil {
		t.Fatal(err)
	}

	batches := make(chan string, 4)
	relay := postgres.NewRelay(h.A, h.Reg, r,
		postgres.WithRelayPollInterval(20*time.Millisecond),
		postgres.WithRelayOnBatch(func(result string, _ time.Duration) { batches <- result }))

	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	runCtx, stop := context.WithCancel(boot)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()

	// Writes land under a request span, after the relay is already polling.
	reqCtx, req := telemetry.Tracer().Start(tctx(t, "acme"), "POST /sites")
	for _, name := range []string{"alice@example.com", "B"} {
		if _, err := x.Exec(reqCtx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	req.End()

	published := 0
	for published < 2 {
		select {
		case result := <-batches:
			if result != "ok" {
				t.Fatalf("batch result = %q", result)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("relay did not publish")
		}
		published = 0
		for _, row := range h.outboxRows(t) {
			if row.Published {
				published++
			}
		}
	}
	stop()
	<-done
	bootSpan.End()

	writers := map[trace.SpanID]bool{}
	for _, s := range telemetrytest.Named(rec, "execute command") {
		writers[s.SpanContext().SpanID()] = true
	}
	batchSpans := telemetrytest.Named(rec, "publish outbox batch")
	if len(batchSpans) == 0 {
		t.Fatal("no publish outbox batch span")
	}
	links := 0
	for _, s := range batchSpans {
		if s.SpanKind() != trace.SpanKindProducer || s.Parent().IsValid() {
			t.Fatalf("want a root producer span, kind=%v parent=%v", s.SpanKind(), s.Parent())
		}
		for _, l := range s.Links() {
			if !writers[l.SpanContext.SpanID()] {
				t.Fatalf("link %v does not lead to an execute command span", l.SpanContext)
			}
			links++
		}
		if s.InstrumentationScope().Name != "github.com/xraph/fabriq" {
			t.Fatalf("scope = %q", s.InstrumentationScope().Name)
		}
	}
	if links != 2 {
		t.Fatalf("got %d links across batches, want 2", links)
	}

	// Nothing the relay did, idle polls included, joined the boot trace.
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == bootSpan.SpanContext().TraceID() && s.Name() != "start fabriq" {
			t.Fatalf("span %q landed in the boot trace", s.Name())
		}
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
}
