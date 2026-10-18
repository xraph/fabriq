package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

func TestStartBatchSpanCapsLinksAndSkipsBadTraceparents(t *testing.T) {
	rec := telemetrytest.Record(t)
	rows := make([]outboxScanRow, 0, 200)
	rows = append(rows, outboxScanRow{Traceparent: ""}, outboxScanRow{Traceparent: "garbage"})
	for i := range 198 {
		rows = append(rows, outboxScanRow{Traceparent: fmt.Sprintf("00-4bf92f3577b34da6a3ce929d0e0e4736-%016x-01", i+1)})
	}
	_, span := startBatchSpan(context.Background(), rows)
	span.End()

	s := telemetrytest.Named(rec, "publish outbox batch")[0]
	if got := len(s.Links()); got != maxBatchLinks {
		t.Fatalf("links = %d, want the cap %d", got, maxBatchLinks)
	}
	if n, _ := telemetrytest.Attr(s, "messaging.batch.message_count"); n != "200" {
		t.Fatalf("messaging.batch.message_count = %q", n)
	}
}
