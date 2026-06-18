//go:build integration

package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	fredis "github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func openRedis(t testing.TB, opts ...fredis.Option) *fredis.Adapter {
	t.Helper()
	addr := fabriqtest.StartRedis(t)
	a, err := fredis.Open(context.Background(), fredis.Config{Addr: addr}, opts...)
	if err != nil {
		t.Fatalf("redis.Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func env(aggID string, version int64) event.Envelope {
	return event.Envelope{
		ID: event.NewID(), TenantID: "acme", Aggregate: "asset", AggID: aggID,
		Version: version, Type: "asset.updated", At: time.Now().UTC(),
		PayloadSchemaVersion: 1, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"version":%d}`, aggID, version)),
	}
}

func TestRedis_PublishWritesEventStreamAndChannels(t *testing.T) {
	a := openRedis(t)
	ctx := context.Background()

	channels := []string{"changes:acme:id:A1", "changes:acme:tenant:acme"}
	streamID, err := a.Publish(ctx, env("A1", 1), channels)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if streamID == "" {
		t.Fatal("Publish must return the event-stream entry id")
	}

	missed, err := a.ReadRange(ctx, "changes:acme:id:A1", "0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(missed) != 1 || missed[0].AggID != "A1" || missed[0].Version != 1 {
		t.Fatalf("channel entry = %+v", missed)
	}
	if missed[0].StreamID == "" || missed[0].Channel != "changes:acme:id:A1" {
		t.Fatalf("delta transport fields missing: %+v", missed[0])
	}
}

func TestRedis_ChannelMaxLenTrims(t *testing.T) {
	a := openRedis(t, fredis.WithChannelMaxLen(100))
	ctx := context.Background()

	for i := 1; i <= 400; i++ {
		if _, err := a.Publish(ctx, env("A1", int64(i)), []string{"changes:acme:id:A1"}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := a.ReadRange(ctx, "changes:acme:id:A1", "0", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) >= 400 {
		t.Fatalf("channel stream not trimmed: %d entries", len(entries))
	}
	if len(entries) < 100 {
		t.Fatalf("over-trimmed: %d entries", len(entries))
	}
}

func TestRedis_TailDeliversLive(t *testing.T) {
	a := openRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan query.Delta, 8)
	go func() {
		_ = a.Tail(ctx, "changes:acme:id:A2", "$", func(d query.Delta) { got <- d })
	}()
	time.Sleep(150 * time.Millisecond) // let the tail attach

	if _, err := a.Publish(ctx, env("A2", 7), []string{"changes:acme:id:A2"}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if d.AggID != "A2" || d.Version != 7 || d.StreamID == "" {
			t.Fatalf("delta = %+v", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tail delivered nothing")
	}
}

func TestRedis_ConsumerGroupAckAndRedelivery(t *testing.T) {
	a := openRedis(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if _, err := a.Publish(ctx, env(fmt.Sprintf("A%d", i), 1), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.EnsureGroup(ctx, "proj:graph"); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// First consumer fails on A2: A1 and A3 ack, A2 stays pending.
	var seen atomic.Int32
	boom := errors.New("apply failed")
	runCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	_ = a.Consume(runCtx, "proj:graph", "c1", func(_ string, e event.Envelope) error {
		seen.Add(1)
		if e.AggID == "A2" {
			return boom
		}
		return nil
	})
	if seen.Load() < 3 {
		t.Fatalf("consumer saw %d events, want 3", seen.Load())
	}

	// A fresh consumer claims the pending entry (at-least-once).
	redelivered := make(chan string, 1)
	runCtx2, stop2 := context.WithTimeout(ctx, 5*time.Second)
	defer stop2()
	_ = a.Consume(runCtx2, "proj:graph", "c2", func(_ string, e event.Envelope) error {
		if e.AggID == "A2" {
			select {
			case redelivered <- e.AggID:
			default:
			}
		}
		return nil
	})
	select {
	case <-redelivered:
	default:
		t.Fatal("failed event was not redelivered")
	}
}

func TestRedis_CacheVersionedTenantKeys(t *testing.T) {
	a := openRedis(t)
	acme, _ := tenant.WithTenant(context.Background(), "acme")
	rival, _ := tenant.WithTenant(context.Background(), "rival")

	cache := a.Cache(3) // model version 3 -> v3 prefix
	if err := cache.Set(acme, "asset", "A1", []byte(`{"name":"pump"}`), time.Minute); err != nil {
		t.Fatal(err)
	}

	val, ok, err := cache.Get(acme, "asset", "A1")
	if err != nil || !ok || string(val) != `{"name":"pump"}` {
		t.Fatalf("Get = (%s, %v, %v)", val, ok, err)
	}

	// Tenant isolation by key prefix.
	if _, ok, _ := cache.Get(rival, "asset", "A1"); ok {
		t.Fatal("cache leaked across tenants")
	}

	// Version bump invalidates without deletes.
	if _, ok, _ := a.Cache(4).Get(acme, "asset", "A1"); ok {
		t.Fatal("old version's entries must be invisible to the new prefix")
	}

	if err := cache.Delete(acme, "asset", "A1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := cache.Get(acme, "asset", "A1"); ok {
		t.Fatal("deleted entry still present")
	}
}

func TestRedis_PresencePubSub(t *testing.T) {
	a := openRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan []byte, 1)
	ready := make(chan struct{})
	go func() {
		_ = a.SubscribePresence(ctx, "room:doc1", func(payload []byte) { got <- payload }, ready)
	}()
	<-ready

	if err := a.PublishPresence(ctx, "room:doc1", []byte(`{"user":"u1","cursor":4}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if string(p) != `{"user":"u1","cursor":4}` {
			t.Fatalf("presence payload = %s", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("presence message not delivered")
	}
}

func TestRedis_ChannelNameMatchesRegistryDerivation(t *testing.T) {
	// The adapter never invents channel names; it publishes to what the
	// registry derives. This pins the contract end to end.
	a := openRedis(t)
	ctx := context.Background()

	ch := registry.ChannelName("acme", registry.ByID, "A9")
	if _, err := a.Publish(ctx, env("A9", 1), []string{ch}); err != nil {
		t.Fatal(err)
	}
	entries, err := a.ReadRange(ctx, ch, "0", 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ReadRange(%s) = %v, %v", ch, entries, err)
	}
}

func TestRedis_TailEvents_ReceivesBroadcast(t *testing.T) {
	a := openRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := make(chan event.Envelope, 4)
	ready := make(chan struct{})
	go func() {
		// Signal that the tailer goroutine is about to start blocking.
		close(ready)
		_ = a.TailEvents(ctx, func(e event.Envelope) error {
			got <- e
			return nil
		})
	}()
	<-ready
	time.Sleep(100 * time.Millisecond) // let the XREAD attach before publishing

	if _, err := a.Publish(ctx, env("broadcast1", 42), nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if e.AggID != "broadcast1" || e.Version != 42 {
			t.Fatalf("TailEvents received wrong envelope: %+v", e)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("TailEvents: handler never received the published envelope")
	}
}

func BenchmarkRedis_Publish(b *testing.B) {
	a := openRedis(b)
	ctx := context.Background()
	channels := []string{"changes:acme:id:BENCH", "changes:acme:tenant:acme"}
	e := env("BENCH", 1)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := a.Publish(ctx, e, channels); err != nil {
			b.Fatal(err)
		}
	}
}
