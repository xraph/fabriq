package forgeext

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/gateway"
)

// httpFakeBackend feeds a controllable delta stream, so the controllers can be
// exercised over a real forge router + HTTP without Redis or a matcher shard.
type httpFakeBackend struct{ deltas chan livequery.LiveDelta }

func (b httpFakeBackend) Subscribe(_ context.Context, _ livequery.LiveQuery) (*gateway.Sub, error) {
	return gateway.NewSub("s1", b.deltas, nil, func() {}), nil
}

// fakeBackedGateway builds a GatewayExtension whose backend is already resolved
// to a fake, bypassing Start (no Redis needed).
func fakeBackedGateway(deltas chan livequery.LiveDelta) *GatewayExtension {
	g := &GatewayExtension{cfg: GatewayConfig{BasePath: "/api/v1/live"}}
	g.backend = httpFakeBackend{deltas: deltas}
	return g
}

// TestGatewaySSE_OverForgeRouter exercises the SSE controller end-to-end over a
// real forge router: POST a LiveQuery, then read the event stream and assert the
// snapshot (reset + enter) is delivered with op names as SSE event names.
func TestGatewaySSE_OverForgeRouter(t *testing.T) {
	app := forge.NewApp(forge.AppConfig{Name: "gw-sse-test", HTTPAddress: ":0"})

	deltas := make(chan livequery.LiveDelta, 4)
	g := fakeBackedGateway(deltas)
	if err := app.RegisterController(newLiveSSEController(g)); err != nil {
		t.Fatalf("register sse controller: %v", err)
	}

	srv := httptest.NewServer(app.Router().Handler())
	defer srv.Close()

	// Snapshot folded into the stream.
	deltas <- livequery.LiveDelta{Op: livequery.OpReset}
	deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a", NewIndex: 0, StreamID: "e1"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/v1/live",
		strings.NewReader(`{"entity":"asset","limit":10}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/v1/live: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	events := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, "event: ") {
				events <- strings.TrimPrefix(line, "event: ")
			}
		}
	}()

	want := []string{"reset", "enter"}
	for i, w := range want {
		select {
		case got := <-events:
			if got != w {
				t.Fatalf("event[%d] = %q, want %q", i, got, w)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for event %q", w)
		}
	}
}

// TestGatewayWS_RouteRegistered asserts the WebSocket endpoint is mounted at
// BasePath+"/ws" (the ServeWS delivery logic itself is covered by the gateway
// package's unit tests against a fake forge.Connection).
func TestGatewayWS_RouteRegistered(t *testing.T) {
	app := forge.NewApp(forge.AppConfig{Name: "gw-ws-test", HTTPAddress: ":0"})
	g := fakeBackedGateway(make(chan livequery.LiveDelta))
	if err := app.RegisterController(newLiveWSController(g)); err != nil {
		t.Fatalf("register ws controller: %v", err)
	}

	var found bool
	for _, r := range app.Router().Routes() {
		if strings.HasSuffix(r.Path, "/api/v1/live/ws") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("WebSocket route /api/v1/live/ws was not registered")
	}
}
