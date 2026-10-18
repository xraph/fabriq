//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

// The document plane's sweep runs on a ticker. A tick with nothing quiet
// records nothing; each quiet document it materializes is one root span, and
// the event it emits carries that span's traceparent.
func TestDocMaterialize_OneRootSpanPerDocument(t *testing.T) {
	owner, a := newDocTypeHarness(t, false)
	docs := a.Documents()
	rec := telemetrytest.Record(t)

	boot, bootSpan := telemetry.Tracer().Start(context.Background(), "start fabriq")
	sweepCtx := telemetry.Detach(boot)
	if n, err := docs.MaterializeQuiet(sweepCtx, nil); err != nil || n != 0 {
		t.Fatalf("idle sweep = (%d, %v)", n, err)
	}
	if got := len(rec.Ended()); got != 0 {
		t.Fatalf("an idle sweep recorded %d spans", got)
	}

	ctx := scopedCtx(t, "ws_1", "")
	docID := "twidget/" + event.NewID()
	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "title", "alice@example.com", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // the entity's quiet window is 10ms

	if n, err := docs.MaterializeQuiet(sweepCtx, nil); err != nil || n != 1 {
		t.Fatalf("MaterializeQuiet = (%d, %v), want 1", n, err)
	}
	bootSpan.End()

	spans := telemetrytest.Named(rec, "materialize document")
	if len(spans) != 1 || spans[0].Parent().IsValid() {
		t.Fatalf("want one root materialize span, got %d", len(spans))
	}
	if e, _ := telemetrytest.Attr(spans[0], "fabriq.entity"); e != "twidget" {
		t.Fatalf("fabriq.entity = %q", e)
	}
	rows, err := owner.Driver().Query(context.Background(), `SELECT traceparent FROM fabriq_outbox WHERE agg_id = $1`, docID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("no outbox row for the materialized document")
	}
	var tp string
	if err := rows.Scan(&tp); err != nil {
		t.Fatal(err)
	}
	if link, ok := telemetry.LinkFromTraceparent(tp); !ok || link.SpanContext.SpanID() != spans[0].SpanContext().SpanID() {
		t.Fatalf("outbox traceparent %q does not name the materialize span", tp)
	}
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == bootSpan.SpanContext().TraceID() && s.Name() != "start fabriq" {
			t.Fatalf("span %q landed in the boot trace", s.Name())
		}
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, docID)
}
