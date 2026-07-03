//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	fredis "github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

func newRelayHarness(t testing.TB) (*harness, *fredis.Adapter) {
	t.Helper()
	h := newHarness(t)
	addr := fabriqtest.StartRedis(t)
	r, err := fredis.Open(context.Background(), fredis.Config{Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return h, r
}

func TestRelay_PublishesOutboxToStreams(t *testing.T) {
	h, r := newRelayHarness(t)
	ctx := tctx(t, "acme")

	site, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "S"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.X.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump", SiteID: site.AggID}}); err != nil {
		t.Fatal(err)
	}

	relay := postgres.NewRelay(h.A, h.Reg, r, postgres.WithRelayPollInterval(50*time.Millisecond))
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- relay.Run(runCtx) }()

	// Outbox drains: all rows published with their stream ids.
	deadline := time.Now().Add(10 * time.Second)
	for {
		rows := h.outboxRows(t)
		published := 0
		for _, row := range rows {
			if row.Published {
				published++
			}
		}
		if published == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay did not drain outbox: %+v", rows)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	<-done

	// Main event stream carries both envelopes...
	events, err := r.ReadRange(context.Background(), registry.StreamKey(), "0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("event stream has %d entries, want 2", len(events))
	}

	// ...and each derived change channel got its copy: the asset event
	// lands on its id channel, its site channel, and the tenant channel.
	for _, channel := range []string{
		"changes:acme:site:" + site.AggID,
		"changes:acme:tenant:acme",
	} {
		entries, err := r.ReadRange(context.Background(), channel, "0", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Fatalf("channel %s received nothing", channel)
		}
	}
}

func TestRelay_WakesOnNotifyQuickly(t *testing.T) {
	h, r := newRelayHarness(t)
	ctx := tctx(t, "acme")

	// Long poll interval: only LISTEN/NOTIFY can make this fast.
	relay := postgres.NewRelay(h.A, h.Reg, r, postgres.WithRelayPollInterval(30*time.Second))
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = relay.Run(runCtx) }()
	time.Sleep(500 * time.Millisecond) // listener attach

	start := time.Now()
	if _, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "N"}}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		rows := h.outboxRows(t)
		if len(rows) == 1 && rows[0].Published {
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Fatalf("publish took %v; NOTIFY wake-up is not working", elapsed)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("event never published")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestElector_ExactlyOneLeaderAndFailover(t *testing.T) {
	h, _ := newRelayHarness(t)

	var active atomic.Int32
	var maxActive atomic.Int32
	lead := func(ctx context.Context) error {
		n := active.Add(1)
		for {
			if m := maxActive.Load(); n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		<-ctx.Done()
		active.Add(-1)
		return ctx.Err()
	}

	const key = int64(424242)
	ctx1, stop1 := context.WithCancel(context.Background())
	ctx2, stop2 := context.WithCancel(context.Background())
	defer stop2()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = postgres.NewElector(h.A, key, postgres.WithElectorRetry(100*time.Millisecond)).Run(ctx1, lead)
	}()
	go func() {
		defer wg.Done()
		_ = postgres.NewElector(h.A, key, postgres.WithElectorRetry(100*time.Millisecond)).Run(ctx2, lead)
	}()

	waitForCond(t, 5*time.Second, func() bool { return active.Load() == 1 })
	time.Sleep(500 * time.Millisecond)
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent leaders = %d, want exactly 1", got)
	}

	// Failover: stop the current leader; the other must take over.
	stop1()
	waitForCond(t, 10*time.Second, func() bool { return active.Load() == 1 })
	stop2()
	wg.Wait()
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("split brain: max concurrent leaders = %d", got)
	}
}

func TestElector_TryLead_LosesCleanlyThenWins(t *testing.T) {
	h, _ := newRelayHarness(t)
	const key = int64(424243)

	// A long-running leader holds the lock…
	holding := make(chan struct{})
	ctx1, stop1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = postgres.NewElector(h.A, key).Run(ctx1, func(ctx context.Context) error {
			close(holding)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-holding

	// …so a TryLead claim loses cleanly: no error, lead never runs.
	ctx := context.Background()
	led, err := postgres.NewElector(h.A, key).TryLead(ctx, func(context.Context) error {
		t.Error("lead ran despite a held lock")
		return nil
	})
	if err != nil || led {
		t.Fatalf("TryLead against a held lock = (%v, %v), want (false, nil)", led, err)
	}

	// Once the holder releases, the claim wins and lead runs.
	stop1()
	wg.Wait()
	ran := false
	waitForCond(t, 5*time.Second, func() bool {
		won, tryErr := postgres.NewElector(h.A, key).TryLead(ctx, func(context.Context) error {
			ran = true
			return nil
		})
		return tryErr == nil && won
	})
	if !ran {
		t.Fatal("lead did not run after the lock was released")
	}
}

func waitForCond(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func BenchmarkRelay_Throughput(b *testing.B) {
	h, r := newRelayHarness(b)
	ctx := tctx(b, "acme")

	cmds := make([]command.Command, 100)
	for i := range cmds {
		cmds[i] = command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: fmt.Sprintf("S%d", i)}}
	}
	relay := postgres.NewRelay(h.A, h.Reg, r, postgres.WithRelayPollInterval(10*time.Millisecond))
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = relay.Run(runCtx) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.X.ExecBatch(ctx, cmds); err != nil {
			b.Fatal(err)
		}
	}
	// Wait for drain so the metric includes publish time.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var unpublished int
		if err := h.A.Driver().QueryRow(context.Background(),
			`SELECT count(*) FROM fabriq_outbox WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
			b.Fatal(err)
		}
		if unpublished == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.ReportMetric(float64(b.N*100), "events")
}
