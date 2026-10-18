package remotegrpc_test

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/telemetry/telemetrytest"
	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

// Dial and ServerOptions trace each RPC: a Client span on the caller's side,
// a Server span continuing the same trace on the far side, and neither
// carries the bearer key or the payload.
func TestRPCsAreTracedWithoutCredentialsOrPayloads(t *testing.T) {
	rec := telemetrytest.Record(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	addr, _ := serveTenant(t)
	f, err := remotegrpc.Dial(context.Background(), fmt.Sprintf("fabriq+grpc://s3cret@%s/acme?tls=false", addr))
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := telemetry.Tracer().Start(context.Background(), "request")
	if _, err := f.Exec(ctx, command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &asset{ID: "asset-1", TenantID: "acme", Name: "alice@example.com"},
	}); err != nil {
		t.Fatal(err)
	}
	span.End()

	var client, server trace.SpanContext
	var serverParent trace.SpanContext
	for _, s := range rec.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindClient:
			client = s.SpanContext()
			if s.Parent().SpanID() != span.SpanContext().SpanID() {
				t.Fatal("the client span must be a child of the caller's span")
			}
		case trace.SpanKindServer:
			server = s.SpanContext()
			serverParent = s.Parent()
		}
	}
	if !client.IsValid() || !server.IsValid() {
		t.Fatalf("want a client and a server span, got %d spans", len(rec.Ended()))
	}
	if server.TraceID() != client.TraceID() || serverParent.SpanID() != client.SpanID() {
		t.Fatal("the server span must continue the client's trace")
	}
	telemetrytest.AssertNoValue(t, rec, "s3cret")
	telemetrytest.AssertNoValue(t, rec, "alice@example.com")
}
