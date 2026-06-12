package subscribe_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/core/tenant"
)

type subAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	SiteID   string `grove:"site_id"`
}

func subRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "asset", Kind: registry.KindAggregate, Model: (*subAsset)(nil), GraphNode: "Asset",
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("site", "site_id"), registry.ByTenant},
	})
	return r
}

func acme(t testing.TB) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func delta(agg, id string, version int64) query.Delta {
	return query.Delta{
		StreamID: fmt.Sprintf("171800000000%d-0", version), Channel: "changes:acme:id:" + id,
		TenantID: "acme", Aggregate: agg, AggID: id, Version: version,
		Type: agg + ".updated", At: time.Now().UTC(), Payload: json.RawMessage(`{}`),
	}
}

// --- channel resolution -----------------------------------------------

func TestResolveChannel(t *testing.T) {
	reg := subRegistry(t)
	cases := []struct {
		name string
		req  query.SubscribeScope
		want string
	}{
		{"by id", query.SubscribeScope{Entity: "asset", Scope: "id", ID: "A1"}, "changes:acme:id:A1"},
		{"by site", query.SubscribeScope{Entity: "asset", Scope: "site", ID: "S1"}, "changes:acme:site:S1"},
		{"by tenant ignores client id", query.SubscribeScope{Entity: "asset", Scope: "tenant", ID: "EVIL"}, "changes:acme:tenant:acme"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := subscribe.ResolveChannel(acme(t), reg, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("ResolveChannel = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveChannel_Failures(t *testing.T) {
	reg := subRegistry(t)
	t.Run("no tenant", func(t *testing.T) {
		_, err := subscribe.ResolveChannel(context.Background(), reg, query.SubscribeScope{Entity: "asset", Scope: "id", ID: "A1"})
		if !errors.Is(err, tenant.ErrNoTenant) {
			t.Fatalf("want ErrNoTenant, got %v", err)
		}
	})
	t.Run("unknown entity", func(t *testing.T) {
		_, err := subscribe.ResolveChannel(acme(t), reg, query.SubscribeScope{Entity: "nope", Scope: "id", ID: "A1"})
		if err == nil {
			t.Fatal("unknown entity must fail")
		}
	})
	t.Run("undeclared scope", func(t *testing.T) {
		_, err := subscribe.ResolveChannel(acme(t), reg, query.SubscribeScope{Entity: "asset", Scope: "warehouse", ID: "W1"})
		if err == nil {
			t.Fatal("scope not declared in the spec must fail")
		}
	})
	t.Run("id scope without id", func(t *testing.T) {
		_, err := subscribe.ResolveChannel(acme(t), reg, query.SubscribeScope{Entity: "asset", Scope: "id"})
		if err == nil {
			t.Fatal("id scope without an id must fail")
		}
	})
}

// ChannelsForEnvelope: an event must be published to its entity channel AND
// every containing-scope channel derived from the payload.
func TestChannelsForEnvelope(t *testing.T) {
	reg := subRegistry(t)
	env := struct {
		agg, id string
		payload map[string]any
	}{"asset", "A1", map[string]any{"id": "A1", "site_id": "S1"}}

	raw, _ := json.Marshal(env.payload)
	channels, err := subscribe.ChannelsForEnvelope(reg, eventEnvelope(env.agg, env.id, raw))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"changes:acme:id:A1",
		"changes:acme:site:S1",
		"changes:acme:tenant:acme",
	}
	if len(channels) != len(want) {
		t.Fatalf("channels = %v, want %v", channels, want)
	}
	for i := range want {
		if channels[i] != want[i] {
			t.Fatalf("channels[%d] = %q, want %q", i, channels[i], want[i])
		}
	}
}

func TestChannelsForEnvelope_EmptyScopeFieldSkipsChannel(t *testing.T) {
	reg := subRegistry(t)
	raw, _ := json.Marshal(map[string]any{"id": "A1", "site_id": ""})
	channels, err := subscribe.ChannelsForEnvelope(reg, eventEnvelope("asset", "A1", raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range channels {
		if strings.Contains(ch, ":site:") {
			t.Fatalf("empty site_id must not derive a site channel: %v", channels)
		}
	}
}

// --- hub + conflation ---------------------------------------------------

func TestHub_ConflatesPerAggregateWithinWindow(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(20 * time.Millisecond))
	defer h.Close()

	ch, cancel, err := h.Subscribe(context.Background(), "changes:acme:id:A1", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	h.Publish("changes:acme:id:A1", delta("asset", "A1", 1))
	h.Publish("changes:acme:id:A1", delta("asset", "A1", 2))
	h.Publish("changes:acme:id:A1", delta("asset", "A1", 3))

	got := receiveN(t, ch, 1, time.Second)
	if got[0].Version != 3 {
		t.Fatalf("conflation must deliver only the latest version, got v%d", got[0].Version)
	}
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra delta: %+v", extra)
	case <-time.After(60 * time.Millisecond):
	}
}

func TestHub_DistinctAggregatesAllSurvive(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(10 * time.Millisecond))
	defer h.Close()

	ch, cancel, err := h.Subscribe(context.Background(), "changes:acme:tenant:acme", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	for i := 1; i <= 3; i++ {
		d := delta("asset", fmt.Sprintf("A%d", i), int64(i))
		d.Channel = "changes:acme:tenant:acme"
		h.Publish("changes:acme:tenant:acme", d)
	}
	got := receiveN(t, ch, 3, time.Second)
	seen := map[string]bool{}
	for _, d := range got {
		seen[d.AggID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("want 3 distinct aggregates, got %v", seen)
	}
}

func TestHub_PublishRawBypassesConflation(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(10 * time.Hour)) // window never fires
	defer h.Close()

	ch, cancel, err := h.Subscribe(context.Background(), "crdt:room:1", 16)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	h.PublishRaw("crdt:room:1", delta("page", "P1", 1))
	got := receiveN(t, ch, 1, time.Second)
	if got[0].AggID != "P1" {
		t.Fatalf("raw publish lost: %+v", got)
	}
}

func TestHub_UnsubscribeStopsDelivery(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(5 * time.Millisecond))
	defer h.Close()

	ch, cancel, err := h.Subscribe(context.Background(), "changes:acme:id:A1", 16)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	h.Publish("changes:acme:id:A1", delta("asset", "A1", 1))

	select {
	case d, ok := <-ch:
		if ok {
			t.Fatalf("delivery after cancel: %+v", d)
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestHub_ContextCancelUnsubscribes(t *testing.T) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(5 * time.Millisecond))
	defer h.Close()

	ctx, cancelCtx := context.WithCancel(context.Background())
	ch, _, err := h.Subscribe(ctx, "changes:acme:id:A1", 16)
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx()

	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed — success
			}
		case <-deadline:
			t.Fatal("subscriber channel not closed after context cancel")
		}
	}
}

// --- authz gate -----------------------------------------------------------

func TestGate_DeniesBeforeChannelResolution(t *testing.T) {
	gate := subscribe.NewGate(subRegistry(t), func(_ context.Context, req query.SubscribeScope) error {
		if req.Entity == "asset" && req.Scope == "tenant" {
			return errors.New("viewer may not watch the whole tenant")
		}
		return nil
	})

	if _, err := gate.Resolve(acme(t), query.SubscribeScope{Entity: "asset", Scope: "tenant"}); err == nil {
		t.Fatal("gate must deny")
	}
	chName, err := gate.Resolve(acme(t), query.SubscribeScope{Entity: "asset", Scope: "id", ID: "A1"})
	if err != nil || chName != "changes:acme:id:A1" {
		t.Fatalf("gate allow path = (%q, %v)", chName, err)
	}
}

// --- SSE bridge -------------------------------------------------------------

func TestSSEWriter_WireFormatAndFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	w, err := subscribe.NewSSEWriter(rec)
	if err != nil {
		t.Fatal(err)
	}
	d := delta("asset", "A1", 4)
	d.StreamID = "1718000000004-0"
	if err := w.WriteDelta(d); err != nil {
		t.Fatal(err)
	}
	if err := w.Heartbeat(); err != nil {
		t.Fatal(err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "id: 1718000000004-0\n") {
		t.Fatalf("missing id line:\n%s", body)
	}
	if !strings.Contains(body, "event: asset.updated\n") {
		t.Fatalf("missing event line:\n%s", body)
	}
	if !strings.Contains(body, "data: {") || !strings.Contains(body, "\n\n") {
		t.Fatalf("missing data/terminator:\n%s", body)
	}
	if !strings.Contains(body, ": ping\n\n") {
		t.Fatalf("missing heartbeat comment:\n%s", body)
	}
	if !rec.Flushed {
		t.Fatal("SSE writer must flush explicitly after every event (proxy-safe)")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatal("X-Accel-Buffering: no required for proxy-safe SSE")
	}
}

func TestLastEventID(t *testing.T) {
	req := httptest.NewRequest("GET", "/subscribe", nil)
	if got := subscribe.LastEventID(req); got != "" {
		t.Fatalf("no header: got %q", got)
	}
	req.Header.Set("Last-Event-ID", "1718000000004-0")
	if got := subscribe.LastEventID(req); got != "1718000000004-0" {
		t.Fatalf("got %q", got)
	}
}

// --- helpers ---------------------------------------------------------------

func receiveN(t *testing.T, ch <-chan query.Delta, n int, _ time.Duration) []query.Delta {
	const timeout = time.Second
	t.Helper()
	out := make([]query.Delta, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case d, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d/%d deltas", len(out), n)
			}
			out = append(out, d)
		case <-deadline:
			t.Fatalf("timeout after %d/%d deltas", len(out), n)
		}
	}
	return out
}

func eventEnvelope(agg, id string, payload json.RawMessage) event.Envelope {
	return event.Envelope{TenantID: "acme", Aggregate: agg, AggID: id, Payload: payload}
}

func BenchmarkConflatorOffer(b *testing.B) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(time.Hour))
	defer h.Close()
	ch, cancel, err := h.Subscribe(context.Background(), "c", 1)
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()
	_ = ch
	d := delta("asset", "A1", 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Publish("c", d)
	}
}

func BenchmarkHubPublish_1kSubscribers(b *testing.B) {
	h := subscribe.NewHub(subscribe.WithConflationWindow(time.Millisecond))
	defer h.Close()
	for i := 0; i < 1000; i++ {
		_, cancel, err := h.Subscribe(context.Background(), "c", 1024)
		if err != nil {
			b.Fatal(err)
		}
		defer cancel()
	}
	d := delta("asset", "A1", 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Publish("c", d)
	}
}
