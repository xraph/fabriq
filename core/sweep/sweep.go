// Package sweep is the catalog-mode worker engine (spec 2026-07-03, D5):
// instead of boot-time loops per shard, a bounded pool sweeps the tenant
// catalog — each ACTIVE tenant's database gets a single maintenance pass
// (relay the outbox, materialize quiet documents, compact due logs) under
// that database's own advisory-lock claim, so any number of worker
// replicas cooperate without duplicate work.
//
// Idle accounting keeps the fleet cheap: a tenant whose pass did no work
// backs off exponentially (base 5s, cap 5min, jittered) in a decaying
// in-memory table, while a wake nudge (published by the facade write
// path) resets the backoff so active tenants get sub-second latency
// without polling everyone.
package sweep

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/catalog"
)

// Result reports what one tenant maintenance pass did.
type Result struct {
	// Relayed is the number of outbox envelopes published.
	Relayed int
	// Materialized is the number of quiet documents materialized.
	Materialized int
	// Compacted is the number of document logs compacted.
	Compacted int
	// Claimed reports whether this replica won the shard's work claims;
	// a false is a clean skip (another replica holds the tenant).
	Claimed bool
}

// Busy reports whether the pass found work — a busy tenant is swept again
// on the very next pass instead of backing off.
func (r Result) Busy() bool {
	return r.Relayed > 0 || r.Materialized > 0 || r.Compacted > 0
}

// Maintainer is one shard's single-pass worker surface: claim the shard's
// advisory locks (non-blocking) and, where won, run one maintenance pass.
// compact asks for the (heavier) document-log compaction sweep too.
type Maintainer interface {
	Sweep(ctx context.Context, compact bool) (Result, error)
}

// TenantSweeper resolves a tenant to its shard and runs one maintenance
// pass against it — the seam between this engine (pure scheduling) and
// the routing/adapter stack.
type TenantSweeper func(ctx context.Context, tenantID string, compact bool) (Result, error)

// Stats summarizes one engine pass.
type Stats struct {
	// Scanned is every catalog entry seen.
	Scanned int
	// Eligible is the active, version-current subset.
	Eligible int
	// Swept is how many tenants had a maintenance pass dispatched.
	Swept int
	// Busy is how many sweeps found work.
	Busy int
	// Errors is how many sweeps failed.
	Errors int
}

// Config tunes the engine. Zero values take the documented defaults.
type Config struct {
	// Workers bounds concurrent tenant sweeps (default 16).
	Workers int
	// ScanInterval is the pass cadence when nothing wakes the engine
	// (default 1s — the poll floor for busy tenants).
	ScanInterval time.Duration
	// SweepTimeout bounds one tenant's maintenance pass (default 1min) so
	// a hung database cannot stall the pool slot forever.
	SweepTimeout time.Duration
	// BackoffBase is the first idle backoff (default 5s).
	BackoffBase time.Duration
	// BackoffCap is the idle backoff ceiling (default 5min).
	BackoffCap time.Duration
	// CompactEvery is the per-tenant document-compaction cadence
	// (default 30s, mirroring the static plane's DocCompactInterval).
	CompactEvery time.Duration
	// MinVersion skips tenants recorded below the binary's migration
	// floor ("" = no gate). The directory enforces the same floor for
	// serving; this keeps the sweeper from burning passes on tenants it
	// cannot acquire anyway.
	MinVersion string
	// OnPass observes each pass's stats (metrics). Called from Pass.
	OnPass func(Stats)
	// OnError observes per-tenant sweep failures (logging/metrics).
	OnError func(tenantID string, err error)
	// Now is the injectable clock (tests).
	Now func() time.Time
	// Jitter returns [0,1) scaling the idle backoff (tests inject 1.0
	// for determinism). Default: equal jitter via math/rand.
	Jitter func() float64
}

// listPage is the catalog page size for scans.
const listPage = 500

// tenantState is the decaying per-tenant idle accounting.
type tenantState struct {
	nextDue     time.Time
	backoff     time.Duration
	lastCompact time.Time
	inFlight    bool
	seen        bool // scratch: marked during a scan, unseen entries decay
}

// Engine schedules tenant sweeps off the catalog.
type Engine struct {
	cat catalog.Catalog
	cfg Config

	mu    sync.Mutex
	sweep TenantSweeper
	state map[string]*tenantState

	wake chan struct{}
}

// New builds an engine over the catalog and the per-tenant sweep seam.
func New(cat catalog.Catalog, sweep TenantSweeper, cfg Config) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = time.Second
	}
	if cfg.SweepTimeout <= 0 {
		cfg.SweepTimeout = time.Minute
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Minute
	}
	if cfg.CompactEvery <= 0 {
		cfg.CompactEvery = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Jitter == nil {
		cfg.Jitter = rand.Float64 // math/rand is fine: jitter, not crypto
	}
	return &Engine{
		cat:   cat,
		cfg:   cfg,
		sweep: sweep,
		state: map[string]*tenantState{},
		wake:  make(chan struct{}, 1),
	}
}

// SetSweepFn swaps the sweep seam (tests/benchmarks).
func (e *Engine) SetSweepFn(fn TenantSweeper) {
	e.mu.Lock()
	e.sweep = fn
	e.mu.Unlock()
}

// Wake marks a tenant due immediately (resetting its idle backoff) and
// nudges the run loop — the write path publishes this so a busy tenant is
// relayed within one pass instead of one backoff window.
func (e *Engine) Wake(tenantID string) {
	e.mu.Lock()
	st := e.state[tenantID]
	if st == nil {
		st = &tenantState{}
		e.state[tenantID] = st
	}
	st.backoff = 0
	st.nextDue = time.Time{}
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// TrackedTenants reports the decaying table's size (tests/metrics).
func (e *Engine) TrackedTenants() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.state)
}

// Run passes until ctx ends: every ScanInterval, or immediately on a wake.
func (e *Engine) Run(ctx context.Context) error {
	for {
		e.Pass(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.wake:
		case <-time.After(e.cfg.ScanInterval):
		}
	}
}

// Pass runs one full scan-and-sweep: list the catalog, dispatch every due
// tenant to the bounded pool, wait for the dispatched sweeps, and report.
// Failures back the tenant off and never abort the pass.
func (e *Engine) Pass(ctx context.Context) Stats {
	var stats Stats
	now := e.cfg.Now()

	e.mu.Lock()
	for _, st := range e.state {
		st.seen = false
	}
	e.mu.Unlock()

	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, e.cfg.Workers)
		smu sync.Mutex // guards stats past this point
	)

	cursor := catalog.Cursor("")
	for {
		page, next, err := e.cat.List(ctx, cursor, listPage)
		if err != nil {
			// Catalog outage: serve what we know, retry next pass.
			break
		}
		for i := range page {
			entry := &page[i]
			stats.Scanned++
			if entry.State != catalog.StateActive {
				continue
			}
			if e.cfg.MinVersion != "" && entry.Version < e.cfg.MinVersion {
				continue
			}
			stats.Eligible++

			e.mu.Lock()
			st := e.state[entry.TenantID]
			if st == nil {
				st = &tenantState{}
				e.state[entry.TenantID] = st
			}
			st.seen = true
			if st.inFlight || now.Before(st.nextDue) {
				e.mu.Unlock()
				continue
			}
			st.inFlight = true
			compact := st.lastCompact.IsZero() || now.Sub(st.lastCompact) >= e.cfg.CompactEvery
			e.mu.Unlock()

			smu.Lock()
			stats.Swept++
			smu.Unlock()

			wg.Add(1)
			sem <- struct{}{}
			go func(tenantID string, st *tenantState, compact bool) {
				defer wg.Done()
				defer func() { <-sem }()

				e.mu.Lock()
				sweepFn := e.sweep
				e.mu.Unlock()

				sctx, cancel := context.WithTimeout(ctx, e.cfg.SweepTimeout)
				res, err := sweepFn(sctx, tenantID, compact)
				cancel()

				done := e.cfg.Now()
				e.mu.Lock()
				st.inFlight = false
				switch {
				case err != nil:
					e.backoffLocked(st, done)
				case res.Busy():
					st.backoff = 0
					st.nextDue = done // due again next pass
				default:
					e.backoffLocked(st, done)
				}
				if err == nil && compact {
					st.lastCompact = done
				}
				e.mu.Unlock()

				smu.Lock()
				if err != nil {
					stats.Errors++
				} else if res.Busy() {
					stats.Busy++
				}
				smu.Unlock()
				if err != nil && e.cfg.OnError != nil {
					e.OnErrorSafe(tenantID, err)
				}
			}(entry.TenantID, st, compact)
		}
		if next == "" {
			break
		}
		cursor = next
	}

	wg.Wait()

	// Decay: drop state for tenants that left the eligible set — a
	// suspended or deleted tenant is owed no bookkeeping.
	e.mu.Lock()
	for tid, st := range e.state {
		if !st.seen && !st.inFlight {
			delete(e.state, tid)
		}
	}
	e.mu.Unlock()

	if e.cfg.OnPass != nil {
		e.cfg.OnPass(stats)
	}
	return stats
}

// backoffLocked bumps a tenant's idle backoff (exponential, capped,
// jittered). Caller holds e.mu.
func (e *Engine) backoffLocked(st *tenantState, now time.Time) {
	if st.backoff <= 0 {
		st.backoff = e.cfg.BackoffBase
	} else {
		st.backoff *= 2
		if st.backoff > e.cfg.BackoffCap {
			st.backoff = e.cfg.BackoffCap
		}
	}
	// Equal jitter: [backoff/2, backoff), full backoff when Jitter()==1.
	half := st.backoff / 2
	st.nextDue = now.Add(half + time.Duration(e.cfg.Jitter()*float64(half)))
}

// OnErrorSafe shields the engine from a panicking observer.
func (e *Engine) OnErrorSafe(tenantID string, err error) {
	defer func() { _ = recover() }()
	e.cfg.OnError(tenantID, err)
}
