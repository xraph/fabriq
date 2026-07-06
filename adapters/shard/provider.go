package shard

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// Router resolves the ctx tenant to a live shard. Acquire pairs the shard
// with a release func: the static Set releases nothing (its pools live for
// the process), while catalog-mode DynamicSet refcounts pooled shards so
// the pool manager never closes a shard mid-operation.
type Router interface {
	Acquire(ctx context.Context) (Shard, func(), error)
	AcquireFor(ctx context.Context, tenantID string) (Shard, func(), error)
}

func noopRelease() {}

// Acquire implements Router on the static Set (no-op release).
func (s *Set) Acquire(ctx context.Context) (Shard, func(), error) {
	sh, err := s.For(ctx)
	return sh, noopRelease, err
}

// AcquireFor implements Router on the static Set (no-op release).
func (s *Set) AcquireFor(ctx context.Context, tenantID string) (Shard, func(), error) {
	sh, err := s.ForTenant(ctx, tenantID)
	return sh, noopRelease, err
}

// Provider owns shard lifecycles in catalog mode: Acquire returns a live
// shard for the id (dialing it on first touch), and the release func MUST
// be called when the operation completes so eviction can make progress.
type Provider interface {
	Acquire(ctx context.Context, shardID string) (Shard, func(), error)
}

// Dialer opens the adapter stack for one tenant database. close tears the
// stack down when the pool manager evicts the shard.
type Dialer func(ctx context.Context, shardID string) (Shard, func() error, error)

// PoolManagerConfig tunes the dynamic pool lifecycle.
type PoolManagerConfig struct {
	// MaxActive caps concurrently-open shards (LRU-evicted, idle first).
	// Zero falls back to 128.
	MaxActive int
	// AcquireTimeout bounds how long Acquire waits for capacity when every
	// open shard is busy. Zero falls back to 5s.
	AcquireTimeout time.Duration
	// DialBackoff is the base negative-cache window after a failed dial
	// (doubled per consecutive failure, capped at 32x). Zero = 2s.
	DialBackoff time.Duration
	// now is injectable for tests.
	now func() time.Time

	// Adaptive, when non-nil, enables autoscaling of MaxActive. The static
	// MaxActive is the default (nil).
	Adaptive *AutoscaleConfig
	// OnScale is called after each cap change (best-effort; must not block).
	OnScale func(ScaleEvent)
	// sampleHeap / newTicker are test seams (nil = production defaults).
	sampleHeap func() uint64
	newTicker  func(time.Duration) (<-chan time.Time, func())
}

// poolCounters are lock-free window counters the autoscaler samples. They
// are monotonic totals; snapshotCounters swaps them to zero per tick.
type poolCounters struct {
	acquires  atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	waits     atomic.Int64
	timeouts  atomic.Int64
}

// poolSignals is one window snapshot fed to the autoscaler (Task 2).
type poolSignals struct {
	acquires, misses, evictions, waits, timeouts int64
	open, held, cap                              int
	heapInUse                                    uint64
}

// PoolManager implements Provider: lazy dial on first touch (singleflight),
// refcounted release, LRU idle-first eviction at the cap, and a per-shard
// dial breaker so a down database cannot be dial-stormed.
type PoolManager struct {
	dial Dialer
	cfg  PoolManagerConfig

	mu      sync.Mutex
	entries map[string]*poolEntry
	freed   chan struct{} // signaled on release/eviction capacity

	cap atomic.Int64 // effective MaxActive (static const, or adaptive)
	ctr poolCounters

	auto       *autoscaler
	onScale    func(ScaleEvent)
	sampleHeap func() uint64
	newTicker  func(time.Duration) (<-chan time.Time, func())
	done       chan struct{}
	closeOnce  sync.Once
}

type poolEntry struct {
	shard    Shard
	close    func() error
	refs     int
	lastUsed time.Time

	// dialing coordinates the singleflight first Acquire.
	dialing chan struct{}
	dialErr error
	ready   bool

	// breaker state.
	failUntil time.Time
	failCount int
}

// NewPoolManager builds a PoolManager over the dialer.
func NewPoolManager(dial Dialer, cfg PoolManagerConfig) *PoolManager {
	if cfg.MaxActive <= 0 {
		cfg.MaxActive = 128
	}
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = 5 * time.Second
	}
	if cfg.DialBackoff <= 0 {
		cfg.DialBackoff = 2 * time.Second
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	p := &PoolManager{
		dial:    dial,
		cfg:     cfg,
		entries: map[string]*poolEntry{},
		freed:   make(chan struct{}, 1),
		onScale: cfg.OnScale,
		done:    make(chan struct{}),
	}
	if cfg.Adaptive != nil {
		p.auto = newAutoscaler(*cfg.Adaptive)
		p.cap.Store(int64(p.auto.cfg.Min)) // start at the floor, grow into the working set
		p.sampleHeap = cfg.sampleHeap
		if p.sampleHeap == nil {
			p.sampleHeap = heapInUse
		}
		p.newTicker = cfg.newTicker
		if p.newTicker == nil {
			p.newTicker = defaultTicker
		}
		go p.runController()
	} else {
		p.cap.Store(int64(cfg.MaxActive))
	}
	return p
}

// Acquire implements Provider.
func (p *PoolManager) Acquire(ctx context.Context, shardID string) (Shard, func(), error) {
	deadline := p.cfg.now().Add(p.cfg.AcquireTimeout)
	waited := false
	for {
		sh, release, retry, err := p.tryAcquire(ctx, shardID)
		if err == nil && !retry {
			return sh, release, nil
		}
		if err != nil {
			return Shard{}, nil, err
		}
		if p.cfg.now().After(deadline) {
			p.ctr.timeouts.Add(1)
			return Shard{}, nil, fabriqerr.New(fabriqerr.CodeUnavailable,
				"shard pool is at capacity.",
				fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"shard": shardID}}))
		}
		if !waited {
			p.ctr.waits.Add(1)
			waited = true
		}
		select {
		case <-ctx.Done():
			return Shard{}, nil, ctx.Err()
		case <-p.freed:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// tryAcquire performs one state-machine step. retry=true means capacity
// was unavailable (caller waits and retries).
func (p *PoolManager) tryAcquire(ctx context.Context, shardID string) (sh Shard, release func(), retry bool, err error) {
	p.mu.Lock()
	e, ok := p.entries[shardID]
	if ok && e.ready {
		e.refs++
		e.lastUsed = p.cfg.now()
		p.ctr.acquires.Add(1)
		p.mu.Unlock()
		return e.shard, p.releaseFunc(shardID), false, nil
	}
	if ok && !e.ready && e.dialing != nil {
		// Someone is dialing; wait for them.
		ch := e.dialing
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return Shard{}, nil, false, ctx.Err()
		}
		return Shard{}, nil, true, nil // loop re-examines the entry
	}
	if ok && p.cfg.now().Before(e.failUntil) {
		p.mu.Unlock()
		return Shard{}, nil, false, fabriqerr.New(fabriqerr.CodeUnavailable,
			"shard is temporarily unreachable (dial breaker open).",
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"shard": shardID}}))
	}

	// Need to dial: ensure capacity first. Evict idle shards until we are
	// below the cap — a LOOP, not a single evict, so a shrunk cap actually
	// sheds connections instead of staying pinned at the old high-water mark
	// (each dial would otherwise evict one and add one, net zero). In steady
	// state live <= cap, so this evicts exactly once, unchanged from before.
	for p.liveCountLocked() >= int(p.cap.Load()) {
		if !p.evictIdleLocked() {
			p.mu.Unlock()
			return Shard{}, nil, true, nil
		}
	}
	if e == nil {
		e = &poolEntry{}
		p.entries[shardID] = e
	}
	ch := make(chan struct{})
	e.dialing = ch
	p.mu.Unlock()

	sh, closeFn, err := p.dial(ctx, shardID)

	p.mu.Lock()
	e.dialing = nil
	close(ch)
	if err != nil {
		e.failCount++
		backoff := p.cfg.DialBackoff << minShift(e.failCount-1, 5)
		e.failUntil = p.cfg.now().Add(backoff)
		e.dialErr = err
		p.mu.Unlock()
		return Shard{}, nil, false, fabriqerr.New(fabriqerr.CodeUnavailable,
			"shard dial failed.", fabriqerr.WithCause(err),
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"shard": shardID}}))
	}
	e.shard, e.close, e.ready = sh, closeFn, true
	e.failCount, e.failUntil = 0, time.Time{}
	e.refs = 1
	e.lastUsed = p.cfg.now()
	p.ctr.acquires.Add(1)
	p.ctr.misses.Add(1)
	p.mu.Unlock()
	return sh, p.releaseFunc(shardID), false, nil
}

func minShift(n, maxShift int) uint {
	if n > maxShift {
		n = maxShift
	}
	if n < 0 {
		n = 0
	}
	return uint(n)
}

func (p *PoolManager) releaseFunc(shardID string) func() {
	released := false // guarded by p.mu; keeps release idempotent
	return func() {
		p.mu.Lock()
		if released {
			p.mu.Unlock()
			return
		}
		released = true
		if e, ok := p.entries[shardID]; ok && e.refs > 0 {
			e.refs--
			e.lastUsed = p.cfg.now()
		}
		p.mu.Unlock()
		select {
		case p.freed <- struct{}{}:
		default:
		}
	}
}

func (p *PoolManager) liveCountLocked() int {
	n := 0
	for _, e := range p.entries {
		if e.ready || e.dialing != nil {
			n++
		}
	}
	return n
}

// evictIdleLocked closes the least-recently-used shard with zero refs.
// Returns false when every open shard is held.
func (p *PoolManager) evictIdleLocked() bool {
	var victimID string
	var victim *poolEntry
	for id, e := range p.entries {
		if !e.ready || e.refs > 0 {
			continue
		}
		if victim == nil || e.lastUsed.Before(victim.lastUsed) {
			victimID, victim = id, e
		}
	}
	if victim == nil {
		return false
	}
	closeFn := victim.close
	delete(p.entries, victimID)
	// Close outside the hot map operations would be nicer, but eviction is
	// cold-path (only when opening a new shard at the cap) and pgx pool
	// Close blocks only until in-flight conns return — and refs==0 means
	// none are.
	if closeFn != nil {
		_ = closeFn()
	}
	p.ctr.evictions.Add(1)
	return true
}

// Stats reports live pool-manager counters (observability).
func (p *PoolManager) Stats() (open, held int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.entries {
		if e.ready {
			open++
			if e.refs > 0 {
				held++
			}
		}
	}
	return open, held
}

// Cap reports the current effective MaxActive (static or adaptive).
func (p *PoolManager) Cap() int { return int(p.cap.Load()) }

// snapshotCounters reads and RESETS the window counters and pairs them with
// a live open/held/cap read. Called once per controller tick.
func (p *PoolManager) snapshotCounters() poolSignals {
	s := poolSignals{
		acquires:  p.ctr.acquires.Swap(0),
		misses:    p.ctr.misses.Swap(0),
		evictions: p.ctr.evictions.Swap(0),
		waits:     p.ctr.waits.Swap(0),
		timeouts:  p.ctr.timeouts.Swap(0),
	}
	p.mu.Lock()
	for _, e := range p.entries {
		if e.ready {
			s.open++
			if e.refs > 0 {
				s.held++
			}
		}
	}
	s.cap = int(p.cap.Load())
	p.mu.Unlock()
	return s
}

func (p *PoolManager) runController() {
	tick, stop := p.newTicker(p.auto.cfg.Interval)
	defer stop()
	for {
		select {
		case <-p.done:
			return
		case <-tick:
			p.reconsider()
		}
	}
}

// reconsider samples one window and applies the autoscaler's decision. A
// panic in the decision core is recovered so the controller keeps ticking
// (the cap simply holds at its last value).
func (p *PoolManager) reconsider() {
	defer func() { _ = recover() }()
	sig := p.snapshotCounters()
	sig.heapInUse = p.sampleHeap()
	newCap, dir, reason := p.auto.decide(sig)
	if dir == scaleHold {
		return
	}
	old := int(p.cap.Load())
	p.cap.Store(int64(newCap))
	if dir == scaleGrow {
		// Nudge any Acquire parked at the old cap to re-check the gate.
		select {
		case p.freed <- struct{}{}:
		default:
		}
	}
	if p.onScale != nil {
		p.onScale(ScaleEvent{OldCap: old, NewCap: newCap, Direction: dir, Reason: reason, Signals: sig})
	}
}

// DynamicSet is the catalog-mode Router: the directory resolves the ctx
// tenant to its dedicated shard id and the provider keeps that shard's
// pools alive.
type DynamicSet struct {
	dir      Directory
	provider Provider
}

// NewDynamicSet builds the catalog-mode router.
func NewDynamicSet(dir Directory, provider Provider) *DynamicSet {
	return &DynamicSet{dir: dir, provider: provider}
}

// Acquire implements Router.
func (d *DynamicSet) Acquire(ctx context.Context) (Shard, func(), error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Shard{}, nil, err
	}
	return d.AcquireFor(ctx, tid)
}

// AcquireFor implements Router.
func (d *DynamicSet) AcquireFor(ctx context.Context, tenantID string) (Shard, func(), error) {
	id, err := d.dir.Shard(ctx, tenantID)
	if err != nil {
		return Shard{}, nil, err
	}
	return d.provider.Acquire(ctx, id)
}

var (
	_ Router   = (*Set)(nil)
	_ Router   = (*DynamicSet)(nil)
	_ Provider = (*PoolManager)(nil)
)

// CloseAll tears down every open shard (process shutdown). Held shards are
// closed too — pgx pool Close blocks until in-flight connections return.
func (p *PoolManager) CloseAll() error {
	p.closeOnce.Do(func() { close(p.done) })
	p.mu.Lock()
	entries := p.entries
	p.entries = map[string]*poolEntry{}
	p.mu.Unlock()
	var firstErr error
	for _, e := range entries {
		if e.ready && e.close != nil {
			if err := e.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
