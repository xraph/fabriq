package fabriq

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	"github.com/xraph/fabriq/core/tenant"
)

// The L1 evict tailer starts inside Open, which forge runs under its boot
// span. Its ctx keeps the caller's values but not its span or cancellation.
func TestBackgroundCtxDropsSpanAndCancellation(t *testing.T) {
	_ = telemetrytest.Record(t)
	parent, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	parent, cancelParent := context.WithCancel(parent)
	parent, span := telemetry.Tracer().Start(parent, "start fabriq")
	defer span.End()

	bg, cancel := backgroundCtx(parent)
	defer cancel()
	if trace.SpanContextFromContext(bg).IsValid() {
		t.Fatal("background ctx kept the boot span")
	}
	if tid, _ := tenant.FromContext(bg); tid != "acme" {
		t.Fatalf("background ctx lost its values: %q", tid)
	}
	cancelParent()
	if bg.Err() != nil {
		t.Fatal("background ctx must outlive its parent")
	}
	cancel()
	if bg.Err() == nil {
		t.Fatal("cancel must stop the background ctx")
	}
}

func TestWithMetricsReachesOpen(t *testing.T) {
	if groveMetricsFrom(nil) != nil {
		t.Fatal("no option, no sink")
	}
	if groveMetricsFrom([]Option{WithConflationWindow(0), WithMetrics(nil)}) != nil {
		t.Fatal("a nil sink stays nil")
	}
}
