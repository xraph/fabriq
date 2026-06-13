//go:build integration

package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

const testSecret = "api-test-secret"

// bootAPI assembles the full stack the api-example binary runs: containers,
// migrations, app role, facade, leader-elected relay, and the server.
func bootAPI(t *testing.T) *server {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:      fabriq.PostgresConfig{DSN: appDSN},
		Redis:         fabriq.RedisConfig{Addr: redisAddr},
		Subscriptions: fabriq.SubscriptionsConfig{ConflationWindow: 30 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis, postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001, postgres.WithElectorRetry(100*time.Millisecond))
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	assets, err := fabriq.For[domain.Asset](f)
	if err != nil {
		t.Fatal(err)
	}
	return &server{fabric: f, auth: newAuthenticator([]byte(testSecret)), assets: assets}
}

func token(t *testing.T, tenantID string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"tenant_id": tenantID, "exp": time.Now().Add(time.Hour).Unix(),
	})
	s, err := tok.SignedString([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// sseFrame is one parsed SSE event.
type sseFrame struct {
	ID    string
	Event string
	Data  string
}

// readFrames reads SSE frames until want are collected or the deadline.
func readFrames(t *testing.T, body *bufio.Reader, want int, timeout time.Duration) []sseFrame {
	t.Helper()
	frames := make([]sseFrame, 0, want)
	cur := sseFrame{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for len(frames) < want {
			line, err := body.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case strings.HasPrefix(line, "id: "):
				cur.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				cur.Event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.Data = strings.TrimPrefix(line, "data: ")
			case line == "" && cur.Event != "":
				frames = append(frames, cur)
				cur = sseFrame{}
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
	return frames
}

func TestAPI_SSE_FetchThenSubscribeWithResume(t *testing.T) {
	srv := bootAPI(t)
	ts := httptest.NewServer(http.HandlerFunc(srv.subscribeHTTP))
	defer ts.Close()

	acmeTok := token(t, "acme")
	tctx, _ := tenant.WithTenant(context.Background(), "acme")

	// Unauthenticated -> 401, never a stream.
	resp, err := http.Get(ts.URL + "?entity=asset&scope=tenant")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", resp.StatusCode)
	}

	// Subscribe (EventSource-style token query param)...
	resp, err = http.Get(ts.URL + "?entity=asset&scope=tenant&token=" + acmeTok)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("subscribe status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatal("missing proxy-safety header")
	}

	// ...then write through the command plane (the REST handlers call the
	// same facade; the transport-level value is in the SSE path).
	reader := bufio.NewReader(resp.Body)
	time.Sleep(300 * time.Millisecond) // pump attach
	created, err := srv.fabric.Exec(tctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump 7", Kind: "pump"}})
	if err != nil {
		t.Fatal(err)
	}

	frames := readFrames(t, reader, 1, 10*time.Second)
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if frames[0].Event != "asset.created" || frames[0].ID == "" {
		t.Fatalf("frame = %+v", frames[0])
	}
	if !strings.Contains(frames[0].Data, created.AggID) {
		t.Fatalf("frame data missing aggregate id: %s", frames[0].Data)
	}
	lastID := frames[0].ID
	resp.Body.Close() // client disconnects

	// While disconnected, the asset changes.
	if _, err := srv.fabric.Exec(tctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: created.AggID,
		Payload: &domain.Asset{Name: "Pump 7b", Kind: "pump"}}); err != nil {
		t.Fatal(err)
	}
	// Give the relay time to publish the missed event.
	time.Sleep(time.Second)

	// Reconnect with Last-Event-ID: the missed update replays first.
	req, _ := http.NewRequest("GET", ts.URL+"?entity=asset&scope=tenant&token="+acmeTok, nil)
	req.Header.Set("Last-Event-ID", lastID)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	frames = readFrames(t, bufio.NewReader(resp2.Body), 1, 10*time.Second)
	if len(frames) != 1 {
		t.Fatalf("resume got %d frames, want the missed update", len(frames))
	}
	if frames[0].Event != "asset.updated" || !strings.Contains(frames[0].Data, `"version":2`) {
		t.Fatalf("resume frame = %+v", frames[0])
	}
}

func TestAPI_SSE_CrossTenantScopeIsolation(t *testing.T) {
	srv := bootAPI(t)
	ts := httptest.NewServer(http.HandlerFunc(srv.subscribeHTTP))
	defer ts.Close()

	rivalTok := token(t, "rival")
	acmeCtx, _ := tenant.WithTenant(context.Background(), "acme")

	resp, err := http.Get(ts.URL + "?entity=asset&scope=tenant&token=" + rivalTok)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	time.Sleep(300 * time.Millisecond)

	if _, err := srv.fabric.Exec(acmeCtx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Secret"}}); err != nil {
		t.Fatal(err)
	}

	frames := readFrames(t, reader, 1, 3*time.Second)
	if len(frames) != 0 {
		t.Fatalf("rival tenant's SSE stream received acme's event: %+v", frames)
	}
}
