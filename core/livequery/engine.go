package livequery

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/tenant"
)

// Feed is the change source for a live query partition (the in-process Redis
// tail bridged to Change; later: per-partition consumer groups).
type Feed interface {
	Changes(ctx context.Context, q LiveQuery, fromWatermark string) (<-chan Change, func(), error)
}

// EngineOptions tunes window cushion and the per-subscription delta buffer.
type EngineOptions struct {
	Cushion int
	Buffer  int
	// Members optionally seeds Streamed subscriptions' membership from the full
	// matching id set; nil falls back to the snapshot page.
	Members MemberLister
}

// Engine runs Maintained and Streamed live queries. Subscriptions to the same
// (tenant, entity) share one partition; identical query shapes within it share
// one view (one window, one matcher) with deltas fanned out — so a saved view
// watched by N clients costs one window, not N.
type Engine struct {
	snap   Snapshotter
	refill Refiller
	feed   Feed
	opts   EngineOptions

	mu         sync.Mutex
	partitions map[string]*partition
	subSeq     uint64
}

// NewEngine wires the engine; zero options fall back to sane defaults.
func NewEngine(snap Snapshotter, refill Refiller, feed Feed, opts EngineOptions) *Engine {
	if opts.Cushion <= 0 {
		opts.Cushion = 2
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 64
	}
	return &Engine{snap: snap, refill: refill, feed: feed, opts: opts, partitions: map[string]*partition{}}
}

// Handle controls one live subscription: tear it down, or slide its window to a
// new anchor for deep/infinite scroll (Reanchor).
type Handle struct {
	eng   *Engine
	p     *partition
	key   string
	subID uint64
	out   chan LiveDelta

	mu   sync.Mutex
	q    LiveQuery
	done bool
}

// Subscribe registers a subscription and returns its snapshot, live delta
// channel, and control handle.
func (e *Engine) Subscribe(ctx context.Context, q LiveQuery) (Snapshot, <-chan LiveDelta, *Handle, error) {
	tid, _ := tenant.FromContext(ctx)
	key := tid + "|" + q.Entity
	p, err := e.partitionFor(ctx, key, q)
	if err != nil {
		return Snapshot{}, nil, nil, err
	}
	subID := atomic.AddUint64(&e.subSeq, 1)
	out := make(chan LiveDelta, e.opts.Buffer)
	snap, aerr := e.attach(ctx, p, q, subID, out)
	if aerr != nil {
		e.release(key)
		return Snapshot{}, nil, nil, aerr
	}
	return snap, out, &Handle{eng: e, p: p, key: key, subID: subID, out: out, q: q}, nil
}

// Close tears the subscription down.
func (h *Handle) Close() {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return
	}
	h.done = true
	h.mu.Unlock()
	h.eng.detach(h.p, h.subID, true)
	h.eng.release(h.key)
}

// Reanchor slides a Maintained subscription's window to a new cursor anchor
// (and optionally a new size) for deep/infinite scroll. It re-keys the
// subscription onto the new shape's view — O(window), not O(result set) — and
// returns the fresh snapshot; the same delta channel then carries the new
// window. The gateway emits OpReset to the client before rendering the result.
func (h *Handle) Reanchor(ctx context.Context, cursor *Cursor, limit int) (Snapshot, error) {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return Snapshot{}, errPartitionClosed
	}
	q2 := h.q
	q2.Cursor = cursor
	if limit > 0 {
		q2.Limit = limit
	}
	h.q = q2
	h.mu.Unlock()

	// Leave the current view (keeping the channel), then attach to the new shape.
	h.eng.detach(h.p, h.subID, false)
	return h.eng.attach(ctx, h.p, q2, h.subID, h.out)
}

// attach joins subID's channel to the view for q's shape, creating and seeding
// the view if it is the first subscriber, and returns the snapshot to render.
func (e *Engine) attach(ctx context.Context, p *partition, q LiveQuery, subID uint64, out chan LiveDelta) (Snapshot, error) {
	sk := shapeKey(q)
	for attempt := 0; attempt < 8; attempt++ {
		var v *view
		creator := false
		var compileErr error
		if derr := p.do(func() {
			v = p.views[sk]
			if v == nil {
				pred, cerr := match.Compile(q.Where)
				if cerr != nil {
					compileErr = cerr
					return
				}
				v = newView(sk, q, pred)
				p.views[sk] = v
				p.index.Add(sk, q.Where)
				creator = true
			}
		}); derr != nil {
			return Snapshot{}, derr
		}
		if compileErr != nil {
			return Snapshot{}, compileErr
		}

		if creator {
			if err := e.seedView(ctx, p, v, q); err != nil {
				_ = p.do(func() {
					if p.views[sk] == v && len(v.subs) == 0 {
						p.index.Remove(sk)
						delete(p.views, sk)
					}
				})
				v.markReady() // unblock any waiters so they retry as creator
				return Snapshot{}, err
			}
		} else {
			select {
			case <-v.readyCh:
			case <-p.done:
				return Snapshot{}, errPartitionClosed
			}
		}

		var snap Snapshot
		attached := false
		if derr := p.do(func() {
			if p.views[sk] != v || !v.ready {
				return // torn down / replaced between ready and attach → retry
			}
			v.subs[subID] = out
			p.subView[subID] = sk
			snap = v.snapshot()
			attached = true
		}); derr != nil {
			return Snapshot{}, derr
		}
		if attached {
			return snap, nil
		}
	}
	return Snapshot{}, errPartitionClosed
}

// seedView runs the creator's snapshot (off the dispatch goroutine) then
// activates the view: it becomes ready and drains the changes buffered during
// the snapshot.
func (e *Engine) seedView(ctx context.Context, p *partition, v *view, q LiveQuery) error {
	if q.Mode == ModeStreamed {
		page, perr := e.snap.Snapshot(ctx, q, max(q.Limit, 1))
		if perr != nil {
			return perr
		}
		ids, ierr := e.memberIDs(ctx, q, page)
		if ierr != nil {
			return ierr
		}
		return p.do(func() {
			for _, m := range ids {
				v.streamMembers[m] = true
			}
			for _, r := range page {
				if r.Version > v.seen[r.AggID] {
					v.seen[r.AggID] = r.Version
				}
			}
			v.initialPage = page
			v.seedMembers(p)
			v.ready = true
			pending := v.pending
			v.pending = nil
			for _, ch := range pending {
				v.applyReady(p, ch)
			}
			v.markReady()
		})
	}

	limit := q.Limit + e.opts.Cushion
	seed, serr := e.snap.Snapshot(ctx, q, limit)
	if serr != nil {
		return serr
	}
	complete := len(seed) < limit
	win, werr := NewWindow(q, v.pred, seed, complete, e.opts.Cushion, e.refill)
	if werr != nil {
		return werr
	}
	return p.do(func() {
		v.win = win
		for _, r := range seed {
			if r.Version > v.seen[r.AggID] {
				v.seen[r.AggID] = r.Version
			}
		}
		v.seedMembers(p)
		v.ready = true
		pending := v.pending
		v.pending = nil
		for _, ch := range pending {
			v.applyReady(p, ch)
		}
		v.markReady()
	})
}

// detach removes subID from its current view; when the view loses its last
// subscriber it is dropped from the index and member map. closeChan closes the
// delta channel (Close); Reanchor passes false to reuse it for the new view.
func (e *Engine) detach(p *partition, subID uint64, closeChan bool) {
	_ = p.do(func() {
		sk, ok := p.subView[subID]
		if !ok {
			return
		}
		delete(p.subView, subID)
		v := p.views[sk]
		if v == nil {
			return
		}
		if out, ok := v.subs[subID]; ok {
			delete(v.subs, subID)
			if closeChan {
				close(out)
			}
		}
		if len(v.subs) == 0 {
			p.index.Remove(sk)
			for aggID := range v.members {
				if ids := p.memberOf[aggID]; ids != nil {
					delete(ids, sk)
					if len(ids) == 0 {
						delete(p.memberOf, aggID)
					}
				}
			}
			delete(p.views, sk)
		}
	})
}

func (e *Engine) partitionFor(ctx context.Context, key string, q LiveQuery) (*partition, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p := e.partitions[key]; p != nil {
		p.refs++
		return p, nil
	}
	// The feed must outlive any single subscriber, so detach its cancellation
	// from the caller's request context while keeping its values (tenant).
	base := context.WithoutCancel(ctx)
	fctx, pcancel := context.WithCancel(base)
	changes, feedCancel, err := e.feed.Changes(fctx, q, "$")
	if err != nil {
		pcancel()
		return nil, err
	}
	p := &partition{
		eng:        e,
		key:        key,
		feedCtx:    fctx,
		feedCancel: feedCancel,
		pcancel:    pcancel,
		changes:    changes,
		ctrl:       make(chan func()),
		done:       make(chan struct{}),
		index:      newPredicateIndex(),
		views:      map[string]*view{},
		memberOf:   map[string]map[string]bool{},
		subView:    map[uint64]string{},
		refs:       1,
	}
	e.partitions[key] = p
	go p.run()
	return p, nil
}

func (e *Engine) release(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.partitions[key]
	if p == nil {
		return
	}
	p.refs--
	if p.refs <= 0 {
		delete(e.partitions, key)
		close(p.done)
	}
}

// Reconcile re-runs every ready Maintained view's query against the snapshot
// oracle (Postgres) and, where the truth diverges from the in-engine window,
// re-seeds the window and emits OpReset so clients re-snapshot. The low-cadence
// drift backstop. Returns the number of views repaired.
func (e *Engine) Reconcile(ctx context.Context) (int, error) {
	e.mu.Lock()
	parts := make([]*partition, 0, len(e.partitions))
	for _, p := range e.partitions {
		parts = append(parts, p)
	}
	e.mu.Unlock()

	type job struct {
		vid string
		q   LiveQuery
	}
	repaired := 0
	for _, p := range parts {
		var jobs []job
		_ = p.do(func() {
			for vid, v := range p.views {
				if v.ready && v.mode == ModeMaintained {
					jobs = append(jobs, job{vid: vid, q: v.q})
				}
			}
		})
		for _, jb := range jobs {
			limit := jb.q.Limit + e.opts.Cushion
			seed, err := e.snap.Snapshot(ctx, jb.q, limit)
			if err != nil {
				return repaired, err
			}
			complete := len(seed) < limit
			var drifted bool
			_ = p.do(func() {
				v := p.views[jb.vid]
				if v == nil || v.win == nil || !windowDrift(v.win, seed, jb.q.Limit) {
					return
				}
				drifted = true
				for aggID := range v.members {
					if ids := p.memberOf[aggID]; ids != nil {
						delete(ids, jb.vid)
						if len(ids) == 0 {
							delete(p.memberOf, aggID)
						}
					}
				}
				v.members = map[string]bool{}
				nw, werr := NewWindow(jb.q, v.pred, seed, complete, e.opts.Cushion, e.refill)
				if werr != nil {
					return
				}
				v.win = nw
				v.seen = map[string]int64{}
				for _, r := range seed {
					if r.Version > v.seen[r.AggID] {
						v.seen[r.AggID] = r.Version
					}
				}
				v.seedMembers(p)
				v.send(LiveDelta{Op: OpReset})
			})
			if drifted {
				repaired++
			}
		}
	}
	return repaired, nil
}

// windowDrift reports whether the window's visible rows disagree with the first
// `n` rows of the truth snapshot by id or version.
func windowDrift(win *Window, truth []Row, n int) bool {
	vis := win.Visible()
	if len(truth) > n {
		truth = truth[:n]
	}
	if len(vis) != len(truth) {
		return true
	}
	for i := range vis {
		if vis[i].AggID != truth[i].AggID || vis[i].Version != truth[i].Version {
			return true
		}
	}
	return false
}

// memberIDs returns the full matching id set for a Streamed subscription, from
// the MemberLister when configured, else the snapshot page.
func (e *Engine) memberIDs(ctx context.Context, q LiveQuery, page []Row) ([]string, error) {
	if e.opts.Members != nil {
		return e.opts.Members.Members(ctx, q)
	}
	ids := make([]string, 0, len(page))
	for _, r := range page {
		ids = append(ids, r.AggID)
	}
	return ids, nil
}
