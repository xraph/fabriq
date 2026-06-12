package subscribe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/subscribe"
)

// fakeTailer records Tail calls and lets the test drive deliveries.
type fakeTailer struct {
	mu      sync.Mutex
	active  map[string]func(query.Delta) // channel -> deliver
	started int
	stopped int
}

func newFakeTailer() *fakeTailer {
	return &fakeTailer{active: map[string]func(query.Delta){}}
}

func (f *fakeTailer) Tail(ctx context.Context, channel, fromID string, deliver func(query.Delta)) error {
	f.mu.Lock()
	f.active[channel] = deliver
	f.started++
	f.mu.Unlock()
	<-ctx.Done()
	f.mu.Lock()
	delete(f.active, channel)
	f.stopped++
	f.mu.Unlock()
	return ctx.Err()
}

func (f *fakeTailer) ReadRange(_ context.Context, channel, afterID string, _ int) ([]query.Delta, error) {
	return []query.Delta{{Channel: channel, StreamID: afterID + "+1", Aggregate: "asset", AggID: "CATCHUP", Version: 1}}, nil
}

func (f *fakeTailer) deliver(channel string, d query.Delta) bool {
	f.mu.Lock()
	deliver, ok := f.active[channel]
	f.mu.Unlock()
	if ok {
		deliver(d)
	}
	return ok
}

func (f *fakeTailer) counts() (started, stopped int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started, f.stopped
}

func TestHub_TailerStartsOnFirstSubscriberAndStopsOnLast(t *testing.T) {
	ft := newFakeTailer()
	h := subscribe.NewHub(subscribe.WithConflationWindow(5*time.Millisecond), subscribe.WithTailer(ft))
	defer h.Close()

	ch1, cancel1, err := h.Subscribe(context.Background(), "changes:acme:id:A1", 16)
	if err != nil {
		t.Fatal(err)
	}
	_, cancel2, err := h.Subscribe(context.Background(), "changes:acme:id:A1", 16)
	if err != nil {
		t.Fatal(err)
	}

	// Pump starts once for the channel.
	waitFor(t, time.Second, func() bool { s, _ := ft.counts(); return s == 1 })

	// Deltas delivered by the tailer reach subscribers through conflation.
	if !ft.deliver("changes:acme:id:A1", delta("asset", "A1", 1)) {
		t.Fatal("tailer not active")
	}
	select {
	case d := <-ch1:
		if d.AggID != "A1" {
			t.Fatalf("delta = %+v", d)
		}
	case <-time.After(time.Second):
		t.Fatal("no delta through pump")
	}

	// Pump survives one of two subscribers leaving...
	cancel1()
	time.Sleep(20 * time.Millisecond)
	if _, stopped := ft.counts(); stopped != 0 {
		t.Fatal("pump stopped while subscribers remain")
	}
	// ...and stops when the last one leaves.
	cancel2()
	waitFor(t, time.Second, func() bool { _, st := ft.counts(); return st == 1 })
}

func TestHub_TailerRestartsForNewSubscriber(t *testing.T) {
	ft := newFakeTailer()
	h := subscribe.NewHub(subscribe.WithConflationWindow(5*time.Millisecond), subscribe.WithTailer(ft))
	defer h.Close()

	_, cancel, err := h.Subscribe(context.Background(), "c", 4)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitFor(t, time.Second, func() bool { _, st := ft.counts(); return st == 1 })

	_, cancel2, err := h.Subscribe(context.Background(), "c", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()
	waitFor(t, time.Second, func() bool { s, _ := ft.counts(); return s == 2 })
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
