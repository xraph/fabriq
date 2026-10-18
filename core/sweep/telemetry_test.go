package sweep_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqtest"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

// Run starts under forge's boot span; neither its catalog scans nor the
// per-tenant sweeps may inherit it, and an idle sweep mints no span.
func TestRunDropsTheInheritedSpan(t *testing.T) {
	rec := telemetrytest.Record(t)
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "acme", catalog.StateActive, "")

	seen := make(chan bool, 1)
	fn := func(ctx context.Context, _ string, _ bool) (sweep.Result, error) {
		select {
		case seen <- trace.SpanContextFromContext(ctx).IsValid():
		default:
		}
		return sweep.Result{Claimed: true}, nil
	}
	eng := sweep.New(cat, fn, sweep.Config{ScanInterval: time.Hour})

	boot, span := telemetry.Tracer().Start(context.Background(), "start fabriq")
	ctx, cancel := context.WithCancel(boot)
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	select {
	case valid := <-seen:
		if valid {
			t.Fatal("the tenant sweep ran under the boot span")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no sweep observed")
	}
	cancel()
	<-done
	span.End()

	if n := len(rec.Ended()); n != 1 {
		t.Fatalf("an idle sweep minted spans: %d ended, want only the boot span", n)
	}
}
