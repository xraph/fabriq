package livequery

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/tenant"
)

// Feed is the change source for a live query partition (P1/P2: the in-process
// Redis tail bridged to Change; later: per-partition consumer groups).
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

// Engine runs Maintained live queries. Subscriptions to the same (tenant,
// entity) share one partition: one feed, one predicate index, one dispatch
// goroutine — so a change is matched against only the subscriptions it can
// affect instead of fanning out linearly across all of them.
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
	return &Engine{
		snap:       snap,
		refill:     refill,
		feed:       feed,
		opts:       opts,
		partitions: map[string]*partition{},
	}
}

// Subscribe registers a Maintained subscription and returns its snapshot and a
// live delta channel. Gapless handoff: the subscription joins the partition's
// shared stream (which buffers changes for it) BEFORE the snapshot is taken,
// then the buffered changes are drained with version-dedup against the snapshot.
func (e *Engine) Subscribe(ctx context.Context, q LiveQuery) (Snapshot, <-chan LiveDelta, func(), error) {
	pred, err := match.Compile(q.Where)
	if err != nil {
		return Snapshot{}, nil, nil, err
	}

	tid, _ := tenant.FromContext(ctx)
	key := tid + "|" + q.Entity

	p, err := e.partitionFor(ctx, key, q)
	if err != nil {
		return Snapshot{}, nil, nil, err
	}

	id := fmt.Sprintf("s%d", atomic.AddUint64(&e.subSeq, 1))
	out := make(chan LiveDelta, e.opts.Buffer)
	s := &liveSub{id: id, mode: q.Mode, pred: pred, sort: q.Sort, out: out, seen: map[string]int64{}, members: map[string]bool{}}
	if q.Mode == ModeStreamed {
		s.streamMembers = map[string]bool{}
	}

	// Join the stream first, so the partition buffers changes for this sub while
	// the snapshot is taken.
	if err := p.do(func() {
		p.subs[id] = s
		p.index.Add(id, q.Where)
	}); err != nil {
		e.release(key)
		return Snapshot{}, nil, nil, err
	}

	cancel := func() {
		e.deregister(p, id)
		e.release(key)
	}
	fail := func(err error) (Snapshot, <-chan LiveDelta, func(), error) {
		e.deregister(p, id)
		e.release(key)
		return Snapshot{}, nil, nil, err
	}

	if q.Mode == ModeStreamed {
		page, perr := e.snap.Snapshot(ctx, q, max(q.Limit, 1))
		if perr != nil {
			return fail(perr)
		}
		ids, ierr := e.memberIDs(ctx, q, page)
		if ierr != nil {
			return fail(ierr)
		}
		if err := p.do(func() {
			for _, m := range ids {
				s.streamMembers[m] = true
				p.setMember(s, m, true)
			}
			for _, r := range page {
				if r.Version > s.seen[r.AggID] {
					s.seen[r.AggID] = r.Version
				}
			}
			s.ready = true
			pending := s.pending
			s.pending = nil
			for _, ch := range pending {
				s.applyReady(p, ch)
			}
		}); err != nil {
			e.release(key)
			return Snapshot{}, nil, nil, err
		}
		return Snapshot{Rows: page}, out, cancel, nil
	}

	// Maintained.
	seed, serr := e.snap.Snapshot(ctx, q, q.Limit+e.opts.Cushion)
	if serr != nil {
		return fail(serr)
	}
	complete := len(seed) < q.Limit+e.opts.Cushion
	win, werr := NewWindow(q, pred, seed, complete, e.opts.Cushion, e.refill)
	if werr != nil {
		return fail(werr)
	}
	snapshot := Snapshot{Rows: win.Visible()}

	// Activate: seed the window, the version high-water, and the member index,
	// then drain the changes buffered during the snapshot.
	if err := p.do(func() {
		s.win = win
		for _, r := range seed {
			if r.Version > s.seen[r.AggID] {
				s.seen[r.AggID] = r.Version
			}
			p.setMember(s, r.AggID, true)
		}
		for _, r := range win.rows {
			p.setMember(s, r.AggID, true)
		}
		s.ready = true
		pending := s.pending
		s.pending = nil
		for _, ch := range pending {
			s.applyReady(p, ch)
		}
	}); err != nil {
		e.release(key)
		return Snapshot{}, nil, nil, err
	}
	return snapshot, out, cancel, nil
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

func (e *Engine) partitionFor(ctx context.Context, key string, q LiveQuery) (*partition, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p := e.partitions[key]; p != nil {
		p.refs++
		return p, nil
	}
	// The feed must outlive any single subscriber, so detach its cancellation
	// from the caller's request context while keeping the context values (tenant).
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
		subs:       map[string]*liveSub{},
		memberOf:   map[string]map[string]bool{},
		refs:       1,
	}
	e.partitions[key] = p
	go p.run()
	return p, nil
}

func (e *Engine) deregister(p *partition, id string) {
	_ = p.do(func() {
		s := p.subs[id]
		if s == nil {
			return
		}
		p.index.Remove(id)
		delete(p.subs, id)
		for aggID := range s.members {
			if ids := p.memberOf[aggID]; ids != nil {
				delete(ids, id)
				if len(ids) == 0 {
					delete(p.memberOf, aggID)
				}
			}
		}
		close(s.out)
	})
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
