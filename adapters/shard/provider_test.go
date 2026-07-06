package shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// fakeDialer records opens/closes and can be told to fail per shard.
type fakeDialer struct {
	mu     sync.Mutex
	opens  map[string]int
	closes map[string]int
	fail   map[string]error
	delay  time.Duration
}

func newFakeDialer() *fakeDialer {
	return &fakeDialer{opens: map[string]int{}, closes: map[string]int{}, fail: map[string]error{}}
}

func (f *fakeDialer) dial(_ context.Context, shardID string) (Shard, func() error, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail[shardID]; err != nil {
		return Shard{}, nil, err
	}
	f.opens[shardID]++
	return Shard{ID: shardID}, func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.closes[shardID]++
		return nil
	}, nil
}

func (f *fakeDialer) counts(shardID string) (opens, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens[shardID], f.closes[shardID]
}

func TestPoolManager_LazyOpensOnce_Singleflight(t *testing.T) {
	d := newFakeDialer()
	d.delay = 20 * time.Millisecond
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 8})
	ctx := context.Background()

	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sh, release, err := p.Acquire(ctx, "s1")
			if err != nil || sh.ID != "s1" {
				failures.Add(1)
				return
			}
			release()
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d acquires failed", failures.Load())
	}
	if opens, _ := d.counts("s1"); opens != 1 {
		t.Fatalf("dialed %d times, want 1 (singleflight)", opens)
	}
}

func TestPoolManager_LRUEvictsIdleFirst_NeverEvictsHeld(t *testing.T) {
	d := newFakeDialer()
	now := time.Unix(0, 0)
	p := NewPoolManager(d.dial, PoolManagerConfig{
		MaxActive: 2, AcquireTimeout: 200 * time.Millisecond,
		now: func() time.Time { now = now.Add(time.Millisecond); return now },
	})
	ctx := context.Background()

	_, r1, err := p.Acquire(ctx, "held")
	if err != nil {
		t.Fatal(err)
	}
	// idle opens second (more recent), then goes idle.
	_, r2, err := p.Acquire(ctx, "idle")
	if err != nil {
		t.Fatal(err)
	}
	r2()

	// Opening a third shard must evict "idle" (refs==0), never "held".
	_, r3, err := p.Acquire(ctx, "third")
	if err != nil {
		t.Fatal(err)
	}
	if _, closes := d.counts("idle"); closes != 1 {
		t.Fatal("idle shard was not evicted")
	}
	if _, closes := d.counts("held"); closes != 0 {
		t.Fatal("held shard must never be evicted")
	}
	r1()
	r3()
}

func TestPoolManager_CapBlocksThenTimesOut(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 1, AcquireTimeout: 120 * time.Millisecond})
	ctx := context.Background()

	_, release, err := p.Acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, _, err = p.Acquire(ctx, "b")
	if fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("err = %v, want CodeUnavailable", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("acquire must wait for capacity before failing")
	}
	release()

	// With capacity released the same acquire succeeds.
	_, r2, err := p.Acquire(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	r2()
}

func TestPoolManager_ReleaseUnblocksWaiter(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 1, AcquireTimeout: 2 * time.Second})
	ctx := context.Background()

	_, release, err := p.Acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, r, err := p.Acquire(ctx, "b")
		if err == nil {
			r()
		}
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never unblocked")
	}
}

func TestPoolManager_DialFailureBreaker(t *testing.T) {
	d := newFakeDialer()
	boom := errors.New("boom")
	d.fail["down"] = boom
	now := time.Unix(1000, 0)
	p := NewPoolManager(d.dial, PoolManagerConfig{
		MaxActive: 4, DialBackoff: time.Second,
		now: func() time.Time { return now },
	})
	ctx := context.Background()

	if _, _, err := p.Acquire(ctx, "down"); !errors.Is(err, boom) {
		t.Fatalf("first dial err = %v, want cause boom", err)
	}
	// Breaker open: no second dial inside the backoff window.
	if _, _, err := p.Acquire(ctx, "down"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("err = %v, want CodeUnavailable", err)
	}
	if opens, _ := d.counts("down"); opens != 0 {
		t.Fatal("failed dials must not count as opens")
	}
	// Recovery after the window.
	d.mu.Lock()
	delete(d.fail, "down")
	d.mu.Unlock()
	now = now.Add(3 * time.Second)
	sh, release, err := p.Acquire(ctx, "down")
	if err != nil || sh.ID != "down" {
		t.Fatalf("post-recovery acquire: %v", err)
	}
	release()
}

func TestPoolManager_DoubleReleaseIsSafe(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 2})
	_, release, err := p.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	release()
	release() // idempotent
	open, held := p.Stats()
	if open != 1 || held != 0 {
		t.Fatalf("stats = open %d held %d, want 1/0", open, held)
	}
}

func TestPoolManager_CountersTrackHitMissEvictWaitTimeout(t *testing.T) {
	d := newFakeDialer()
	now := time.Unix(0, 0)
	p := NewPoolManager(d.dial, PoolManagerConfig{
		MaxActive: 1, AcquireTimeout: 60 * time.Millisecond,
		now: func() time.Time { now = now.Add(time.Millisecond); return now },
	})
	ctx := context.Background()

	// miss #1: cold dial of "a".
	_, rel, err := p.Acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	// timeout: cap 1, "a" held, "b" cannot get capacity.
	if _, _, err := p.Acquire(ctx, "b"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("want CodeUnavailable, got %v", err)
	}
	rel() // release "a" so it becomes idle-evictable
	// miss #2: "b" now dials, evicting idle "a".
	_, rel2, err := p.Acquire(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	rel2()
	// hit: re-acquire "b" (ready).
	_, rel3, err := p.Acquire(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	rel3()

	s := p.snapshotCounters()
	if s.acquires != 3 { // a, b(after evict), b(hit)
		t.Errorf("acquires=%d want 3", s.acquires)
	}
	if s.misses != 2 { // a, b
		t.Errorf("misses=%d want 2", s.misses)
	}
	if s.evictions != 1 { // a evicted for b
		t.Errorf("evictions=%d want 1", s.evictions)
	}
	if s.waits < 1 { // the "b" that timed out waited
		t.Errorf("waits=%d want >=1", s.waits)
	}
	if s.timeouts != 1 {
		t.Errorf("timeouts=%d want 1", s.timeouts)
	}
	if s.cap != 1 {
		t.Errorf("cap=%d want 1", s.cap)
	}
}

func TestPoolManager_SnapshotResetsWindow(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 4})
	_, rel, _ := p.Acquire(context.Background(), "a")
	rel()
	first := p.snapshotCounters()
	if first.acquires == 0 {
		t.Fatal("expected acquires in first window")
	}
	second := p.snapshotCounters()
	if second.acquires != 0 || second.misses != 0 {
		t.Fatalf("window not reset: %+v", second)
	}
	if second.open != 1 { // "a" still open (open/held are live, not windowed)
		t.Fatalf("open=%d want 1", second.open)
	}
}

func TestPoolManager_CapIsAtomic_GrowUnblocks(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 1, AcquireTimeout: 2 * time.Second})
	ctx := context.Background()
	_, rel, err := p.Acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	done := make(chan error, 1)
	go func() {
		_, r, err := p.Acquire(ctx, "b")
		if err == nil {
			r()
		}
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)
	p.cap.Store(2) // grow: "b" can now open without "a" releasing
	select {
	case p.freed <- struct{}{}: // nudge the waiter (mirrors reconsider on grow)
	default:
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter failed after grow: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("grow did not unblock the waiter")
	}
}

func TestPoolManager_CapIsAtomic_ShrinkEvicts(t *testing.T) {
	d := newFakeDialer()
	now := time.Unix(0, 0)
	p := NewPoolManager(d.dial, PoolManagerConfig{
		MaxActive: 3, AcquireTimeout: 200 * time.Millisecond,
		now: func() time.Time { now = now.Add(time.Millisecond); return now },
	})
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		_, rel, err := p.Acquire(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		rel() // all idle
	}
	if open, _ := p.Stats(); open != 3 {
		t.Fatalf("open=%d want 3", open)
	}
	p.cap.Store(1) // shrink
	// Opening a new shard must evict down toward the new cap (idle-first).
	_, rel, err := p.Acquire(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	rel()
	open, _ := p.Stats()
	if open > 2 { // at most the just-opened "d" plus at most cap-1 survivors
		t.Fatalf("shrink did not evict: open=%d want <=2", open)
	}
}

// staticDirectory routes every tenant to a fixed shard id (test helper).
type staticDirectory map[string]string

func (s staticDirectory) Shard(_ context.Context, tenantID string) (string, error) {
	id, ok := s[tenantID]
	if !ok {
		return "", fabriqerr.New(fabriqerr.CodeNotFound, "unknown tenant.")
	}
	return id, nil
}

func TestDynamicSet_RoutesByTenant(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 4})
	ds := NewDynamicSet(staticDirectory{"t1": "c1/db1", "t2": "c1/db2"}, p)

	sh, release, err := ds.AcquireFor(context.Background(), "t1")
	if err != nil || sh.ID != "c1/db1" {
		t.Fatalf("t1 → %v (%v)", sh.ID, err)
	}
	release()
	sh, release, err = ds.AcquireFor(context.Background(), "t2")
	if err != nil || sh.ID != "c1/db2" {
		t.Fatalf("t2 → %v (%v)", sh.ID, err)
	}
	release()
	if _, _, err := ds.AcquireFor(context.Background(), "ghost"); fabriqerr.CodeOf(err) != fabriqerr.CodeNotFound {
		t.Fatalf("ghost err = %v, want CodeNotFound", err)
	}
}

// BenchmarkDynamicSet_AcquireHot is the request hot path with a resolved,
// cached shard. Spec target: < 200 ns/op, ≤ 1 alloc.
func BenchmarkDynamicSet_AcquireHot(b *testing.B) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 4})
	ds := NewDynamicSet(staticDirectory{"t1": "c1/db1"}, p)
	ctx := context.Background()
	if _, r, err := ds.AcquireFor(ctx, "t1"); err != nil {
		b.Fatal(err)
	} else {
		r()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, release, err := ds.AcquireFor(ctx, "t1")
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

// blockingTicker never fires — lets tests drive reconsider() by hand while
// the background goroutine idles.
func blockingTicker(time.Duration) (<-chan time.Time, func()) {
	return make(chan time.Time), func() {}
}

func TestController_GrowStoresCapAndFiresEvent(t *testing.T) {
	d := newFakeDialer()
	var events []ScaleEvent
	p := NewPoolManager(d.dial, PoolManagerConfig{
		Adaptive: &AutoscaleConfig{
			Min: 2, Max: 100, ConfirmTicks: 1, CooldownTicks: 0,
			MissRatioHigh: 0.20,
		},
		OnScale:    func(ev ScaleEvent) { events = append(events, ev) },
		sampleHeap: func() uint64 { return 0 },
		newTicker:  blockingTicker,
	})
	defer p.CloseAll()
	if p.Cap() != 2 {
		t.Fatalf("start cap=%d want Min 2", p.Cap())
	}
	// Simulate a pressured window.
	p.ctr.acquires.Store(100)
	p.ctr.misses.Store(50)
	p.mu.Lock()
	p.entries["x"] = &poolEntry{ready: true}
	p.mu.Unlock()

	p.reconsider()
	if p.Cap() != 3 { // ceil(2*1.5)
		t.Fatalf("cap=%d want 3 after grow", p.Cap())
	}
	if len(events) != 1 || events[0].Direction != scaleGrow {
		t.Fatalf("events=%+v want one grow", events)
	}
}

func TestController_ShrinkOnIdle(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{
		Adaptive: &AutoscaleConfig{
			Min: 2, Max: 100, ConfirmTicks: 1, CooldownTicks: 0,
		},
		sampleHeap: func() uint64 { return 0 },
		newTicker:  blockingTicker,
	})
	defer p.CloseAll()
	p.cap.Store(32) // oversized, idle
	p.reconsider()
	if p.Cap() != 31 {
		t.Fatalf("cap=%d want 31 after idle shrink", p.Cap())
	}
}

func TestController_HeapCriticalShrinks(t *testing.T) {
	d := newFakeDialer()
	heap := uint64(0)
	p := NewPoolManager(d.dial, PoolManagerConfig{
		Adaptive: &AutoscaleConfig{
			Min: 2, Max: 100, ConfirmTicks: 1, CooldownTicks: 5,
			HeapSoftLimit: 1000, HeapShrinkMult: 1.10,
		},
		sampleHeap: func() uint64 { return heap },
		newTicker:  blockingTicker,
	})
	defer p.CloseAll()
	p.cap.Store(20)
	heap = 5000 // critical
	p.reconsider()
	if p.Cap() != 19 {
		t.Fatalf("cap=%d want 19 after heap-critical shrink", p.Cap())
	}
}

func TestController_StopsOnClose(t *testing.T) {
	d := newFakeDialer()
	ticks := make(chan time.Time)
	stopped := make(chan struct{})
	p := NewPoolManager(d.dial, PoolManagerConfig{
		Adaptive:   &AutoscaleConfig{Min: 2, Max: 8, Interval: time.Hour},
		sampleHeap: func() uint64 { return 0 },
		newTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() { close(stopped) }
		},
	})
	if err := p.CloseAll(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("controller ticker was not stopped on CloseAll")
	}
}

func TestPoolManager_AdaptiveDisabled_NoController(t *testing.T) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 42})
	if p.auto != nil {
		t.Fatal("autoscaler must be nil when adaptive is disabled")
	}
	if p.Cap() != 42 {
		t.Fatalf("cap=%d want static 42", p.Cap())
	}
}

// BenchmarkPoolManager_ChurnLRU: zipf-ish access over more shards than the
// cap; asserts a healthy hit ratio for the hot set.
func BenchmarkPoolManager_ChurnLRU(b *testing.B) {
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 16, AcquireTimeout: time.Second})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 90% of traffic on 8 hot shards, 10% across 64 cold ones.
		var id string
		if i%10 != 0 {
			id = fmt.Sprintf("hot-%d", i%8)
		} else {
			id = fmt.Sprintf("cold-%d", i%64)
		}
		_, release, err := p.Acquire(ctx, id)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
	b.StopTimer()
	totalOpens := 0
	d.mu.Lock()
	for id, n := range d.opens {
		if len(id) > 3 && id[:3] == "hot" {
			totalOpens += n
		}
	}
	d.mu.Unlock()
	if b.N > 1000 && totalOpens > 8*3 {
		b.Fatalf("hot shards re-dialed %d times — LRU is thrashing the hot set", totalOpens)
	}
}
