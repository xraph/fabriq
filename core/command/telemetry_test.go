package command_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/otel"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
)

type observed struct{ op, result string }

type recordingObserver struct {
	mu  sync.Mutex
	got []observed
}

func (o *recordingObserver) ObserveCommand(op, result string, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.got = append(o.got, observed{op, result})
}

func TestExecBatchSpanAndObserver(t *testing.T) {
	rec := telemetrytest.Record(t)
	obs := &recordingObserver{}
	store := newFakeStore()
	x, err := command.NewExecutor(cmdRegistry(t), store,
		command.WithObserver(obs),
		command.WithTraceparent(otel.TraceparentFromContext))
	if err != nil {
		t.Fatal(err)
	}

	ctx, req := telemetry.Tracer().Start(acmeCtx(t), "request")
	created, err := x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "alice@example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := int64(9)
	_, _ = x.Exec(ctx, command.Command{Entity: "site", Op: command.OpUpdate, AggID: created.AggID,
		Payload: &cmdSite{Name: "B"}, ExpectedVersion: &stale})
	_, _ = x.ExecBatch(ctx, []command.Command{
		{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "C"}},
		{Entity: "nope", Op: command.OpDelete, AggID: "x"},
	})
	req.End()

	spans := telemetrytest.Named(rec, "execute command")
	if len(spans) != 3 {
		t.Fatalf("got %d execute command spans, want 3", len(spans))
	}
	for _, s := range spans {
		if s.SpanKind() != trace.SpanKindInternal {
			t.Fatalf("kind = %v", s.SpanKind())
		}
		if s.Parent().SpanID() != req.SpanContext().SpanID() {
			t.Fatal("execute command must be a child of the caller's span")
		}
	}
	wantAttrs := []map[string]string{
		{"fabriq.command.op": "create", "fabriq.entity": "site", "fabriq.batch_size": "1", "fabriq.result": "ok"},
		{"fabriq.command.op": "update", "fabriq.entity": "site", "fabriq.batch_size": "1", "fabriq.result": "conflict"},
		{"fabriq.command.op": "mixed", "fabriq.batch_size": "2", "fabriq.result": "invalid"},
	}
	for i, want := range wantAttrs {
		for k, v := range want {
			if got, _ := telemetrytest.Attr(spans[i], k); got != v {
				t.Fatalf("span %d %s = %q, want %q", i, k, got, v)
			}
		}
		if spans[i].Status().Description != "" {
			t.Fatalf("span %d: a %s outcome is not a span error: %+v", i, want["fabriq.result"], spans[i].Status())
		}
	}
	if _, ok := telemetrytest.Attr(spans[2], "fabriq.entity"); ok {
		t.Fatal("a batch over several entities must not name one")
	}

	// The envelope's stamped traceparent names the execute command span, so
	// the relay's and consumers' links lead back to it.
	tp := store.outbox[0].Traceparent
	if link, ok := telemetry.LinkFromTraceparent(tp); !ok || link.SpanContext.SpanID() != spans[0].SpanContext().SpanID() {
		t.Fatalf("envelope traceparent %q does not name the execute command span", tp)
	}

	want := []observed{{"create", "ok"}, {"update", "conflict"}, {"mixed", "invalid"}}
	if len(obs.got) != len(want) {
		t.Fatalf("observer got %v, want %v", obs.got, want)
	}
	for i := range want {
		if obs.got[i] != want[i] {
			t.Fatalf("observation %d = %v, want %v", i, obs.got[i], want[i])
		}
	}
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
	telemetrytest.AssertNoValue(t, rec, "acme")
}

func TestExecNoTenantIsInvalid(t *testing.T) {
	rec := telemetrytest.Record(t)
	obs := &recordingObserver{}
	x, err := command.NewExecutor(cmdRegistry(t), newFakeStore(), command.WithObserver(obs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Exec(context.Background(), command.Command{Entity: "site", Op: command.OpCreate, Payload: &cmdSite{Name: "A"}}); err == nil {
		t.Fatal("want ErrNoTenant")
	}
	if len(obs.got) != 1 || obs.got[0] != (observed{"create", "invalid"}) {
		t.Fatalf("observer got %v", obs.got)
	}
	if s := telemetrytest.Named(rec, "execute command"); len(s) != 1 || s[0].Parent().IsValid() {
		t.Fatalf("spans = %v", s)
	}
}
