package fabriq

import (
	"context"
	"testing"
	"time"

	gometrics "github.com/xraph/go-utils/metrics"
	"github.com/xraph/grove/observability"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/registry"
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
	if openGroveMetrics(nil, registry.New()) != nil {
		t.Fatal("without WithMetrics, Open must dial grove with no sink")
	}
	rec := &labelRecorder{Metrics: gometrics.NewMockMetrics()}
	sink := openGroveMetrics([]Option{WithConflationWindow(time.Second), WithMetrics(rec), WithStreamMaxLen(10)}, registry.New())
	if sink == nil {
		t.Fatal("WithMetrics did not reach the sink Open dials grove with")
	}
	// What Open hands grove records on the caller's sink, collection label
	// already bounded.
	sink.Histogram(observability.OperationDurationMetric,
		gometrics.WithLabels(map[string]string{collectionLabel: "tenant_acme.fabriq_outbox", "db.system.name": "postgresql"}))
	if len(rec.labels) != 1 || rec.labels[0][collectionLabel] != "fabriq_outbox" {
		t.Fatalf("recorded labels = %v", rec.labels)
	}
}
