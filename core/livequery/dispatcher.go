package livequery

import (
	"context"
	"errors"

	"github.com/xraph/fabriq/core/livequery/match"
)

var errPartitionClosed = errors.New("fabriq: live query partition closed")

// liveSub is one subscription's state inside a partition. Every field is owned
// by the partition's single dispatch goroutine — there are no locks.
type liveSub struct {
	id      string
	pred    match.Predicate
	out     chan LiveDelta
	win     *Window
	seen    map[string]int64 // per-aggregate version high-water (gapless dedup)
	members map[string]bool  // aggIDs this sub currently holds (reverse of memberOf)
	ready   bool
	pending []Change
}

// send delivers a delta without ever blocking the shared dispatch goroutine: a
// consumer that falls behind is told to re-snapshot rather than stalling its
// neighbours.
func (s *liveSub) send(d LiveDelta) {
	select {
	case s.out <- d:
	default:
		select {
		case s.out <- LiveDelta{Op: OpReset, At: d.At}:
		default:
		}
	}
}

func (s *liveSub) apply(p *partition, ch Change) {
	if !s.ready {
		s.pending = append(s.pending, ch)
		return
	}
	s.applyReady(p, ch)
}

func (s *liveSub) applyReady(p *partition, ch Change) {
	if v, ok := s.seen[ch.AggID]; ok && ch.Version <= v {
		return // already reflected (gapless dedup)
	}
	s.seen[ch.AggID] = ch.Version
	before := s.win.Contains(ch.AggID)
	deltas := s.win.Apply(p.feedCtx, ch)
	after := s.win.Contains(ch.AggID)
	if after != before {
		p.setMember(s, ch.AggID, after)
	}
	for _, d := range deltas {
		d.StreamID = ch.StreamID
		d.At = ch.At
		s.send(d)
	}
}

// partition owns the shared feed, predicate index, and per-subscription state
// for one (tenant, entity). A single goroutine (run) processes both feed
// changes and control commands, so all mutable state below is lock-free.
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
	subs     map[string]*liveSub
	memberOf map[string]map[string]bool // aggID -> subIDs holding it

	refs int // guarded by Engine.mu
}

func (p *partition) setMember(s *liveSub, aggID string, member bool) {
	if member {
		s.members[aggID] = true
		ids := p.memberOf[aggID]
		if ids == nil {
			ids = map[string]bool{}
			p.memberOf[aggID] = ids
		}
		ids[s.id] = true
		return
	}
	delete(s.members, aggID)
	if ids := p.memberOf[aggID]; ids != nil {
		delete(ids, s.id)
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

// dispatch routes one change to only the subscriptions it can affect: the
// content-index candidates (rows whose new state could match) unioned with the
// current holders of the aggregate (to catch leaves, where the new state no
// longer matches but the row must still be removed).
func (p *partition) dispatch(ch Change) {
	cands := p.index.Candidates(ch.Vals)
	for id := range p.memberOf[ch.AggID] {
		cands[id] = true
	}
	for id := range cands {
		if s := p.subs[id]; s != nil {
			s.apply(p, ch)
		}
	}
}

// do runs fn on the dispatch goroutine and waits for it, so callers mutate
// partition state without locks. It fails if the partition has stopped.
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
