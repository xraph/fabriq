//go:build loadtest

package fabriq_test

// Catalog-mode load baseline (spec P6): many tenant databases across two
// container clusters behind ONE facade with a bounded pool, mixed
// read/write zipfian traffic, the sweeper running throughout.
//
//	go test -tags loadtest -run TestLoad_CatalogMode -v -timeout 60m .
//
// Tunables (env): FABRIQ_LOADTEST_TENANTS (default 500),
// FABRIQ_LOADTEST_SECONDS (default 15), FABRIQ_LOADTEST_WORKERS (default 32).
// The committed defaults are the baseline; smaller values smoke-test the
// harness.
//
// Observed (2026-07-03): the default 500 needs REAL clusters. On two
// laptop-sized timescaledb-ha containers the run collapsed at ~140
// databases per cluster — provisioning degraded from ~170ms to seconds
// per tenant and the maintenance DB stopped accepting connections
// (Timescale spawns per-database scheduler workers; autovacuum scales
// with database count). That wall IS the ops guidance — hundreds of
// databases per cluster, sized for it — so the committed local baseline
// runs FABRIQ_LOADTEST_TENANTS=200 (100/cluster).
//
// Baseline (2026-07-03, M3 Max, 200 tenants / 2 clusters / cap 64 /
// 32 workers / 15s zipfian):
//
//	provision: 47s total (236 ms/tenant, 16-way)
//	reads:  n=12544 p50=13.1ms p99=97.3ms
//	writes: n=5729  p50=23.1ms p99=147.3ms
//	errors: 0 — pool pegged at cap (64 open vs 200 tenants, 3x
//	        oversubscribed; LRU eviction churn absorbed without a 503)
//	sweep:  172ms full pass over 200 tenants; fleet drained 1.0s after
//	        traffic stopped
//
// Adaptive A/B (set FABRIQ_LOADTEST_ADAPTIVE=1; Adaptive{Min:16, Max:128,
// Interval:1s}): starts the pool at Min=16 and grows into the zipf hot set
// instead of pegging a hand-tuned static cap. The no-Docker convergence
// harness (adapters/shard: TestAdaptive_BeatsFixedCap_UnderZipf) is the
// committed proof — over 40k ops on 200 shards, zipf(1.2): fixed cap 16
// dials 16756 times (hit ratio 0.58) vs adaptive 3178 dials (hit ratio
// 0.92), cap converging into (16,128]. On real clusters the adaptive facade
// should match/beat the static hit ratio starting from a smaller Min, hold
// open<=live cap, and keep 0 errors. (Fill the p50/p99/final-cap row from a
// real two-cluster run.)

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

func TestLoad_CatalogMode(t *testing.T) {
	nTenants := envInt("FABRIQ_LOADTEST_TENANTS", 500)
	seconds := envInt("FABRIQ_LOADTEST_SECONDS", 15)
	workers := envInt("FABRIQ_LOADTEST_WORKERS", 32)
	const maxActive = 64
	adaptive := os.Getenv("FABRIQ_LOADTEST_ADAPTIVE") == "1"

	ctx := context.Background()
	c1 := fabriqtest.StartPostgres(t) // doubles as the control DB
	c2 := fabriqtest.StartPostgres(t)
	clusters := map[string]string{"c1": c1, "c2": c2}

	// ---- Provision the fleet (parallel, provisioning is idempotent). ----
	cat, err := postgres.OpenCatalog(ctx, c1)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(clusters)
	p := provision.New(cat, ops)

	provStart := time.Now()
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var provErrs atomic.Int64
	tenants := make([]string, nTenants)
	for i := 0; i < nTenants; i++ {
		tid := fmt.Sprintf("load%04d", i)
		tenants[i] = tid
		cluster := "c1"
		if i%2 == 1 {
			cluster = "c2"
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(tid, cluster string) {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := p.Provision(ctx, tid, cluster); err != nil {
				provErrs.Add(1)
				t.Errorf("provision %s: %v", tid, err)
				return
			}
			tdsn, _ := ops.TenantDSN(cluster, "fabriq_"+tid)
			fabriqtest.ApplyDDL(t, tdsn, cmDDL())
		}(tid, cluster)
	}
	wg.Wait()
	if provErrs.Load() > 0 {
		t.Fatalf("%d tenants failed to provision", provErrs.Load())
	}
	provDur := time.Since(provStart)
	_ = cat.Close()

	// ---- One facade over the fleet, pool capped far below the fleet. ----
	reg := cmRegistry(t)
	catCfg := fabriq.CatalogConfig{
		DSN: c1, ClusterDSNs: clusters,
		MaxActiveShards: maxActive,
		AllowSuperuser:  true,
	}
	if adaptive {
		catCfg.Adaptive = fabriq.AdaptivePoolConfig{
			Enabled:  true,
			Min:      16,
			Max:      maxActive * 2, // 128
			Interval: time.Second,   // converge within the 15s window
		}
	}
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{Catalog: catCfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// The sweeper runs throughout, exactly like production.
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	var lastPass atomic.Value // sweep.Stats
	sweeper := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		ScanInterval: 500 * time.Millisecond,
		MinVersion:   migrations.HeadVersion(),
		OnPass:       func(st sweep.Stats) { lastPass.Store(st) },
	})
	go func() { _ = sweeper.Run(runCtx) }()

	// ---- Mixed zipfian traffic: 70% reads, 30% writes. ----
	type sample struct {
		read bool
		d    time.Duration
	}
	var mu sync.Mutex
	var samples []sample
	var reads, writes, errsN atomic.Int64
	seedIDs := sync.Map{} // tenant -> a created agg id (read targets)

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var twg sync.WaitGroup
	for w := 0; w < workers; w++ {
		twg.Add(1)
		go func(seed int64) {
			defer twg.Done()
			rng := rand.New(rand.NewSource(seed))
			zipf := rand.NewZipf(rng, 1.2, 1.0, uint64(nTenants-1))
			for time.Now().Before(deadline) {
				tid := tenants[int(zipf.Uint64())]
				tctx, _ := tenant.WithTenant(ctx, tid)
				id, hasID := seedIDs.Load(tid)
				start := time.Now()
				if hasID && rng.Float64() < 0.7 {
					var got cmWidget
					err = f.Relational().Get(tctx, "cmwidget", id.(string), &got)
					if err == nil {
						reads.Add(1)
						mu.Lock()
						samples = append(samples, sample{read: true, d: time.Since(start)})
						mu.Unlock()
					} else {
						errsN.Add(1)
					}
					continue
				}
				res, execErr := f.Exec(tctx, command.Command{
					Entity: "cmwidget", Op: command.OpCreate,
					Payload: &cmWidget{Name: "load-" + tid},
				})
				if execErr != nil {
					errsN.Add(1)
					continue
				}
				seedIDs.LoadOrStore(tid, res.AggID)
				writes.Add(1)
				mu.Lock()
				samples = append(samples, sample{read: false, d: time.Since(start)})
				mu.Unlock()
			}
		}(int64(w) + 1)
	}
	twg.Wait()

	// ---- Drain: how long until the sweeper reports the fleet idle? ----
	drainStart := time.Now()
	drainDeadline := drainStart.Add(5 * time.Minute)
	var drainDur time.Duration
	for {
		st, _ := lastPass.Load().(sweep.Stats)
		if st.Busy == 0 && st.Errors == 0 && st.Scanned >= nTenants && time.Since(drainStart) > time.Second {
			drainDur = time.Since(drainStart)
			break
		}
		if time.Now().After(drainDeadline) {
			t.Fatalf("fleet never drained: last pass %+v", st)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// ---- Report. ----
	var readL, writeL []time.Duration
	for _, s := range samples {
		if s.read {
			readL = append(readL, s.d)
		} else {
			writeL = append(writeL, s.d)
		}
	}
	sort.Slice(readL, func(i, j int) bool { return readL[i] < readL[j] })
	sort.Slice(writeL, func(i, j int) bool { return writeL[i] < writeL[j] })
	open, held, _ := stores.PoolStats()
	st, _ := lastPass.Load().(sweep.Stats)

	t.Logf("=== catalog-mode load baseline ===")
	t.Logf("tenants=%d clusters=2 poolCap=%d workers=%d duration=%ds", nTenants, maxActive, workers, seconds)
	t.Logf("provision: %v total (%.0f ms/tenant, 16-way)", provDur.Round(time.Second), float64(provDur.Milliseconds())/float64(nTenants))
	t.Logf("reads:  n=%d p50=%v p99=%v", reads.Load(), percentile(readL, 0.50), percentile(readL, 0.99))
	t.Logf("writes: n=%d p50=%v p99=%v", writes.Load(), percentile(writeL, 0.50), percentile(writeL, 0.99))
	t.Logf("errors: %d (pool-cap 503s count here by design)", errsN.Load())
	t.Logf("pool: open=%d held=%d (cap %d)", open, held, maxActive)
	capv, _ := stores.PoolCap()
	t.Logf("adaptive=%v finalCap=%d (static cap=%d)", adaptive, capv, maxActive)
	t.Logf("sweep: last pass %+v; drain-after-traffic=%v", st, drainDur.Round(time.Millisecond))

	if reads.Load() == 0 || writes.Load() == 0 {
		t.Fatal("traffic generator produced no successful operations")
	}
	if !adaptive && open > maxActive {
		t.Fatalf("pool exceeded its cap: open=%d cap=%d", open, maxActive)
	}
	if adaptive {
		if capv > maxActive*2 {
			t.Fatalf("adaptive cap exceeded ceiling: cap=%d ceiling=%d", capv, maxActive*2)
		}
		if open > capv {
			t.Fatalf("open exceeded the live adaptive cap: open=%d cap=%d", open, capv)
		}
	}
}
