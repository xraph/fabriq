package livequery

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

var errPartitionClosed = errors.New("fabriq: live query partition closed")

// shapeKey identifies queries that are byte-for-byte the same shape (within a
// partition, where tenant+entity already match). Identical shapes — e.g. many
// clients on the same saved view — share one view: one window, one matcher.
func shapeKey(q LiveQuery) string {
	b, _ := json.Marshal(struct {
		M Mode        `json:"m"`
		L int         `json:"l"`
		S []SortKey   `json:"s"`
		W query.Where `json:"w"`
		C *Cursor     `json:"c"`
	}{q.Mode, q.Limit, q.Sort, q.Where, q.Cursor})
	return string(b)
}

// view is the shared matcher+window for one query shape inside a partition.
// Every field is owned by the partition's single dispatch goroutine — no locks.
// Multiple subscribers (subs) attach to one view; deltas fan out to all of them.
type view struct {
	id   string
	q    LiveQuery
	pred match.Predicate
	mode Mode

	win           *Window
	streamMembers map[string]bool // Streamed mode membership ID-set

	initialPage []Row // Streamed-mode initial render page

	seen      map[string]int64 // per-aggregate version high-water (gapless dedup)
	members   map[string]bool  // aggIDs this view holds (reverse of memberOf)
	ready     bool
	readyCh   chan struct{}
	readyOnce sync.Once
	pending   []Change

	subs map[uint64]chan LiveDelta // attached subscriber channels
}

// markReady closes readyCh exactly once (the activate path and the teardown
// path may both reach for it).
func (v *view) markReady() { v.readyOnce.Do(func() { close(v.readyCh) }) }

func newView(id string, q LiveQuery, pred match.Predicate) *view {
	v := &view{
		id: id, q: q, pred: pred, mode: q.Mode,
		seen: map[string]int64{}, members: map[string]bool{},
		readyCh: make(chan struct{}), subs: map[uint64]chan LiveDelta{},
	}
	if q.Mode == ModeStreamed {
		v.streamMembers = map[string]bool{}
	}
	return v
}

// send fans a delta to every attached subscriber without blocking the shared
// dispatch goroutine; a consumer that falls behind is told to re-snapshot.
func (v *view) send(d LiveDelta) {
	for _, out := range v.subs {
		select {
		case out <- d:
		default:
			select {
			case out <- LiveDelta{Op: OpReset, At: d.At}:
			default:
			}
		}
	}
}

func (v *view) contains(aggID string) bool {
	if v.mode == ModeStreamed {
		return v.streamMembers[aggID]
	}
	return v.win != nil && v.win.Contains(aggID)
}

func (v *view) apply(p *partition, ch Change) {
	if !v.ready {
		v.pending = append(v.pending, ch)
		return
	}
	v.applyReady(p, ch)
}

func (v *view) applyReady(p *partition, ch Change) {
	if seen, ok := v.seen[ch.AggID]; ok && ch.Version <= seen {
		return // gapless dedup
	}
	v.seen[ch.AggID] = ch.Version
	before := v.contains(ch.AggID)
	var deltas []LiveDelta
	if v.mode == ModeStreamed {
		deltas = v.applyStreamed(ch)
	} else {
		deltas = v.win.Apply(p.feedCtx, ch)
	}
	after := v.contains(ch.AggID)
	if after != before {
		p.setMember(v, ch.AggID, after)
	}
	for _, d := range deltas {
		d.StreamID = ch.StreamID
		d.At = ch.At
		v.send(d)
	}
}

func (v *view) applyStreamed(ch Change) []LiveDelta {
	matches := !ch.Deleted && v.pred.Eval(ch.Vals)
	was := v.streamMembers[ch.AggID]
	switch {
	case matches && !was:
		v.streamMembers[ch.AggID] = true
		return []LiveDelta{{Op: OpMatch, AggID: ch.AggID, Version: ch.Version, Row: ch.Raw, Cursor: SortKeyOf(ch.Vals, v.q.Sort, ch.AggID)}}
	case !matches && was:
		delete(v.streamMembers, ch.AggID)
		return []LiveDelta{{Op: OpUnmatch, AggID: ch.AggID, Version: ch.Version}}
	case matches && was:
		return []LiveDelta{{Op: OpUpdate, AggID: ch.AggID, Version: ch.Version, Row: ch.Raw, Cursor: SortKeyOf(ch.Vals, v.q.Sort, ch.AggID)}}
	}
	return nil
}

// snapshot returns the current state a newly-attached subscriber renders.
func (v *view) snapshot() Snapshot {
	if v.mode == ModeStreamed {
		// Streamed: the creator's initial page (with payloads) for first render;
		// further state arrives via match/unmatch deltas and cursor paging.
		return Snapshot{Rows: v.initialPage}
	}
	if v.win == nil {
		return Snapshot{}
	}
	return Snapshot{Rows: v.win.Visible()}
}

// seedMembers wires the view's current holdings into the partition reverse index.
func (v *view) seedMembers(p *partition) {
	if v.mode == ModeStreamed {
		for id := range v.streamMembers {
			p.setMember(v, id, true)
		}
		return
	}
	for _, r := range v.win.rows {
		p.setMember(v, r.AggID, true)
	}
}

// partition owns the shared feed, predicate index, and views for one
// (tenant, entity). A single goroutine (run) processes feed changes and control
// commands, so all mutable state below is lock-free.
type partition struct {
	eng        *Engine
	key        string
	feedCtx    context.Context
	feedCancel func()
	pcancel    context.CancelFunc
	changes    <-chan Change
	ctrl       chan func()
	done       chan struct{}

	index    *predicateIndex
	views    map[string]*view
	memberOf map[string]map[string]bool // aggID -> viewIDs holding it
	subView  map[uint64]string          // subID -> current viewID (shapeKey)

	refs int // guarded by Engine.mu
}

func (p *partition) setMember(v *view, aggID string, member bool) {
	if member {
		v.members[aggID] = true
		ids := p.memberOf[aggID]
		if ids == nil {
			ids = map[string]bool{}
			p.memberOf[aggID] = ids
		}
		ids[v.id] = true
		return
	}
	delete(v.members, aggID)
	if ids := p.memberOf[aggID]; ids != nil {
		delete(ids, v.id)
		if len(ids) == 0 {
			delete(p.memberOf, aggID)
		}
	}
}

func (p *partition) run() {
	defer p.pcancel()
	defer p.feedCancel()
	for {
		select {
		case <-p.done:
			return
		case fn := <-p.ctrl:
			fn()
		case ch, ok := <-p.changes:
			if !ok {
				return
			}
			p.dispatch(ch)
		}
	}
}

func (p *partition) dispatch(ch Change) {
	cands := p.index.Candidates(ch.Vals)
	for id := range p.memberOf[ch.AggID] {
		cands[id] = true
	}
	for id := range cands {
		if v := p.views[id]; v != nil {
			v.apply(p, ch)
		}
	}
}

// do runs fn on the dispatch goroutine and waits for it.
func (p *partition) do(fn func()) error {
	done := make(chan struct{})
	select {
	case p.ctrl <- func() { fn(); close(done) }:
	case <-p.done:
		return errPartitionClosed
	}
	select {
	case <-done:
		return nil
	case <-p.done:
		return errPartitionClosed
	}
}
