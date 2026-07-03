package sweep_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/fabriqtest"
)

// seed puts one catalog entry in the fake with the given state/version.
func seed(t testing.TB, cat catalog.Catalog, tenant string, state catalog.State, version string) {
	t.Helper()
	e, err := cat.Put(context.Background(), catalog.Entry{
		TenantID: tenant, ClusterID: "c1", Database: "fabriq_" + tenant,
		State: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != "" {
		e.Version = version
		if _, err := cat.Put(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
}

// recordingSweep counts sweeps per tenant, thread-safe.
type recordingSweep struct {
	mu     sync.Mutex
	counts map[string]int
	result sweep.Result
	errFor map[string]error
}

func newRecordingSweep(result sweep.Result) *recordingSweep {
	return &recordingSweep{counts: map[string]int{}, result: result, errFor: map[string]error{}}
}

func (r *recordingSweep) fn(_ context.Context, tenantID string, _ bool) (sweep.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[tenantID]++
	if err := r.errFor[tenantID]; err != nil {
		return sweep.Result{}, err
	}
	return r.result, nil
}

func (r *recordingSweep) count(tenantID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[tenantID]
}

func TestSweeper_VisitsEveryActiveTenant(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	for i := 0; i < 10; i++ {
		seed(t, cat, fmt.Sprintf("active%02d", i), catalog.StateActive, "")
	}
	seed(t, cat, "asleep", catalog.StateSuspended, "")
	seed(t, cat, "broken", catalog.StateFailed, "")
	seed(t, cat, "coming", catalog.StatePending, "")

	rec := newRecordingSweep(sweep.Result{Claimed: true})
	eng := sweep.New(cat, rec.fn, sweep.Config{})

	stats := eng.Pass(context.Background())
	if stats.Scanned != 13 || stats.Eligible != 10 || stats.Swept != 10 {
		t.Fatalf("stats = %+v, want scanned=13 eligible=10 swept=10", stats)
	}
	for i := 0; i < 10; i++ {
		if got := rec.count(fmt.Sprintf("active%02d", i)); got != 1 {
			t.Fatalf("active%02d swept %d times, want 1", i, got)
		}
	}
	for _, skip := range []string{"asleep", "broken", "coming"} {
		if got := rec.count(skip); got != 0 {
			t.Fatalf("%s swept %d times, want 0", skip, got)
		}
	}
}

func TestSweeper_SkipsSuspendedAndVersionGated(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "current", catalog.StateActive, "0030")
	seed(t, cat, "stale", catalog.StateActive, "0010")
	seed(t, cat, "paused", catalog.StateSuspended, "0030")

	rec := newRecordingSweep(sweep.Result{Claimed: true})
	eng := sweep.New(cat, rec.fn, sweep.Config{MinVersion: "0030"})

	stats := eng.Pass(context.Background())
	if stats.Swept != 1 {
		t.Fatalf("stats = %+v, want exactly 1 swept", stats)
	}
	if rec.count("current") != 1 || rec.count("stale") != 0 || rec.count("paused") != 0 {
		t.Fatalf("counts = %v", rec.counts)
	}
}

func TestSweeper_BoundedConcurrency(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	for i := 0; i < 24; i++ {
		seed(t, cat, fmt.Sprintf("t%02d", i), catalog.StateActive, "")
	}

	var inFlight, peak int64
	fn := func(context.Context, string, bool) (sweep.Result, error) {
		n := atomic.AddInt64(&inFlight, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if n <= p || atomic.CompareAndSwapInt64(&peak, p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
		return sweep.Result{Claimed: true}, nil
	}

	eng := sweep.New(cat, fn, sweep.Config{Workers: 3})
	stats := eng.Pass(context.Background())
	if stats.Swept != 24 {
		t.Fatalf("swept = %d, want 24", stats.Swept)
	}
	if got := atomic.LoadInt64(&peak); got > 3 {
		t.Fatalf("peak concurrency = %d, want <= 3", got)
	}
}

func TestSweeper_BackoffIdle_And_WakeOnNudge(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "acme", catalog.StateActive, "")

	rec := newRecordingSweep(sweep.Result{Claimed: true}) // idle: no work done
	now := time.Unix(1000, 0)
	eng := sweep.New(cat, rec.fn, sweep.Config{
		BackoffBase: 5 * time.Second,
		BackoffCap:  5 * time.Minute,
		Now:         func() time.Time { return now },
		Jitter:      func() float64 { return 1.0 }, // deterministic: full backoff
	})
	ctx := context.Background()

	// First pass sweeps; the tenant reported idle, so it backs off.
	eng.Pass(ctx)
	if rec.count("acme") != 1 {
		t.Fatalf("count = %d, want 1", rec.count("acme"))
	}
	eng.Pass(ctx)
	if rec.count("acme") != 1 {
		t.Fatalf("count = %d after immediate re-pass, want 1 (backed off)", rec.count("acme"))
	}

	// The base backoff elapses: swept again, then backs off doubled (10s).
	now = now.Add(5 * time.Second)
	eng.Pass(ctx)
	if rec.count("acme") != 2 {
		t.Fatalf("count = %d after base backoff, want 2", rec.count("acme"))
	}
	now = now.Add(5 * time.Second)
	eng.Pass(ctx)
	if rec.count("acme") != 2 {
		t.Fatalf("count = %d inside doubled backoff, want 2", rec.count("acme"))
	}

	// A wake nudge cuts through the backoff immediately.
	eng.Wake("acme")
	eng.Pass(ctx)
	if rec.count("acme") != 3 {
		t.Fatalf("count = %d after wake, want 3", rec.count("acme"))
	}

	// A busy tenant never backs off: due again on the very next pass.
	rec.result = sweep.Result{Claimed: true, Relayed: 7}
	now = now.Add(5 * time.Second)
	eng.Pass(ctx)
	eng.Pass(ctx)
	if rec.count("acme") != 5 {
		t.Fatalf("count = %d for busy tenant, want 5 (swept every pass)", rec.count("acme"))
	}
}

func TestSweeper_BackoffCaps(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "acme", catalog.StateActive, "")

	rec := newRecordingSweep(sweep.Result{Claimed: true})
	now := time.Unix(1000, 0)
	eng := sweep.New(cat, rec.fn, sweep.Config{
		BackoffBase: 5 * time.Second,
		BackoffCap:  20 * time.Second,
		Now:         func() time.Time { return now },
		Jitter:      func() float64 { return 1.0 },
	})
	ctx := context.Background()

	// Walk the backoff ladder past the cap: 5s, 10s, 20s, 20s, ...
	sweeps := 0
	for i := 0; i < 6; i++ {
		eng.Pass(ctx)
		sweeps = rec.count("acme")
		now = now.Add(20 * time.Second) // always >= cap
	}
	if sweeps != 6 {
		t.Fatalf("count = %d, want 6 (capped backoff keeps tenant on a 20s cadence)", sweeps)
	}
}

func TestSweeper_LockLosersSkipCleanly(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	for i := 0; i < 8; i++ {
		seed(t, cat, fmt.Sprintf("t%d", i), catalog.StateActive, "")
	}

	// A shared "advisory lock table": exactly one sweeper wins each tenant.
	var mu sync.Mutex
	held := map[string]bool{}
	won := map[string]int{}
	contested := func(_ context.Context, tenantID string, _ bool) (sweep.Result, error) {
		mu.Lock()
		if held[tenantID] {
			mu.Unlock()
			return sweep.Result{Claimed: false}, nil // lock loser: clean skip
		}
		held[tenantID] = true
		mu.Unlock()

		time.Sleep(time.Millisecond)

		mu.Lock()
		won[tenantID]++
		held[tenantID] = false
		mu.Unlock()
		return sweep.Result{Claimed: true, Relayed: 1}, nil
	}

	a := sweep.New(cat, contested, sweep.Config{})
	b := sweep.New(cat, contested, sweep.Config{})
	var wg sync.WaitGroup
	var statsA, statsB sweep.Stats
	wg.Add(2)
	go func() { defer wg.Done(); statsA = a.Pass(context.Background()) }()
	go func() { defer wg.Done(); statsB = b.Pass(context.Background()) }()
	wg.Wait()

	if statsA.Errors != 0 || statsB.Errors != 0 {
		t.Fatalf("lock losers must not error: a=%+v b=%+v", statsA, statsB)
	}
	mu.Lock()
	defer mu.Unlock()
	for tid, n := range won {
		if n < 1 || n > 2 {
			t.Fatalf("tenant %s won %d times across two single-pass sweepers", tid, n)
		}
	}
	if len(won) != 8 {
		t.Fatalf("only %d tenants did work, want 8", len(won))
	}
}

func TestSweeper_OneFailingTenantDoesNotStarveOthers(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	for i := 0; i < 5; i++ {
		seed(t, cat, fmt.Sprintf("ok%d", i), catalog.StateActive, "")
	}
	seed(t, cat, "bad", catalog.StateActive, "")

	rec := newRecordingSweep(sweep.Result{Claimed: true, Relayed: 1}) // busy: due every pass
	rec.errFor["bad"] = fmt.Errorf("tenant database is down")

	var errTenants []string
	now := time.Unix(1000, 0)
	eng := sweep.New(cat, rec.fn, sweep.Config{
		BackoffBase: 5 * time.Second,
		Now:         func() time.Time { return now },
		Jitter:      func() float64 { return 1.0 },
		OnError: func(tenantID string, _ error) {
			errTenants = append(errTenants, tenantID)
		},
	})
	ctx := context.Background()

	// Three consecutive passes: the healthy (busy) tenants are swept every
	// pass; the failing tenant is reported, backed off, and never blocks.
	for i := 0; i < 3; i++ {
		eng.Pass(ctx)
	}
	for i := 0; i < 5; i++ {
		if got := rec.count(fmt.Sprintf("ok%d", i)); got != 3 {
			t.Fatalf("ok%d swept %d times, want 3", i, got)
		}
	}
	if got := rec.count("bad"); got != 1 {
		t.Fatalf("failing tenant swept %d times across immediate passes, want 1 (backed off)", got)
	}
	if len(errTenants) != 1 || errTenants[0] != "bad" {
		t.Fatalf("OnError calls = %v", errTenants)
	}
}

func TestSweeper_CompactCadence(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "acme", catalog.StateActive, "")

	var compacts, sweeps int
	var mu sync.Mutex
	fn := func(_ context.Context, _ string, compact bool) (sweep.Result, error) {
		mu.Lock()
		defer mu.Unlock()
		sweeps++
		if compact {
			compacts++
		}
		return sweep.Result{Claimed: true, Relayed: 1}, nil // busy: swept every pass
	}

	now := time.Unix(1000, 0)
	eng := sweep.New(cat, fn, sweep.Config{
		CompactEvery: 30 * time.Second,
		Now:          func() time.Time { return now },
	})
	ctx := context.Background()

	// First sweep compacts (never compacted before); the next ones inside
	// the window do not; past the window it compacts again.
	eng.Pass(ctx)
	now = now.Add(time.Second)
	eng.Pass(ctx)
	now = now.Add(time.Second)
	eng.Pass(ctx)
	now = now.Add(30 * time.Second)
	eng.Pass(ctx)

	mu.Lock()
	defer mu.Unlock()
	if sweeps != 4 || compacts != 2 {
		t.Fatalf("sweeps=%d compacts=%d, want 4 sweeps with compact on the 1st and 4th", sweeps, compacts)
	}
}

func TestSweeper_DropsStateForRemovedTenants(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "keeper", catalog.StateActive, "")
	seed(t, cat, "leaver", catalog.StateActive, "")

	rec := newRecordingSweep(sweep.Result{Claimed: true})
	eng := sweep.New(cat, rec.fn, sweep.Config{})
	ctx := context.Background()

	eng.Pass(ctx)
	if eng.TrackedTenants() != 2 {
		t.Fatalf("tracked = %d, want 2", eng.TrackedTenants())
	}

	// The tenant leaves the catalog (suspend counts too: no work owed).
	e, err := cat.Get(ctx, "leaver")
	if err != nil {
		t.Fatal(err)
	}
	e.State = catalog.StateSuspended
	if _, err := cat.Put(ctx, e); err != nil {
		t.Fatal(err)
	}
	eng.Pass(ctx)
	if eng.TrackedTenants() != 1 {
		t.Fatalf("tracked = %d after suspension, want 1 (decaying table)", eng.TrackedTenants())
	}
}

func TestSweeper_RunHonorsWakeAndShutdown(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seed(t, cat, "acme", catalog.StateActive, "")

	swept := make(chan string, 16)
	fn := func(_ context.Context, tenantID string, _ bool) (sweep.Result, error) {
		select {
		case swept <- tenantID:
		default:
		}
		return sweep.Result{Claimed: true}, nil
	}

	// A long scan interval: only the initial pass and wakes sweep.
	eng := sweep.New(cat, fn, sweep.Config{ScanInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	waitSweep := func(label string) {
		t.Helper()
		select {
		case <-swept:
		case <-time.After(5 * time.Second):
			t.Fatalf("no sweep observed: %s", label)
		}
	}
	waitSweep("initial pass")
	eng.Wake("acme")
	waitSweep("wake nudge")

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop on cancel")
	}
}

// BenchmarkSweeper_Scan10kIdleTenants: one full pass over 10k catalog
// entries with 100 due and the rest backed off. Spec target: < 100 ms,
// O(1) allocs per idle skip (allocs must track pages + due tenants, not
// the 9.9k idle skips).
func BenchmarkSweeper_Scan10kIdleTenants(b *testing.B) {
	cat := fabriqtest.NewFakeCatalog()
	for i := 0; i < 10_000; i++ {
		seed(b, cat, fmt.Sprintf("t%05d", i), catalog.StateActive, "")
	}

	now := time.Unix(1000, 0)
	var swept int64
	fn := func(context.Context, string, bool) (sweep.Result, error) {
		atomic.AddInt64(&swept, 1)
		return sweep.Result{Claimed: true, Relayed: 1}, nil // busy: stays due
	}
	eng := sweep.New(cat, fn, sweep.Config{
		BackoffBase: time.Hour, // idle tenants stay parked for the whole run
		Now:         func() time.Time { return now },
	})

	// Prime: every tenant swept once; then park all but 100 by marking
	// them idle (they back off an hour).
	ctx := context.Background()
	eng.Pass(ctx)
	fnIdle := func(_ context.Context, tenantID string, _ bool) (sweep.Result, error) {
		if tenantID >= "t00100" {
			return sweep.Result{Claimed: true}, nil // idle: backs off
		}
		return sweep.Result{Claimed: true, Relayed: 1}, nil
	}
	eng.SetSweepFn(fnIdle)
	eng.Pass(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := eng.Pass(ctx)
		if stats.Swept != 100 {
			b.Fatalf("swept = %d, want 100", stats.Swept)
		}
	}
}
