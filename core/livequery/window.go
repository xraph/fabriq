package livequery

import (
	"context"
	"sort"

	"github.com/xraph/fabriq/core/livequery/match"
)

// Window maintains an ordered prefix of a live query's result: the visible
// window [0,N) plus a cushion [N,N+C). Invariant: rows are exactly the first
// len(rows) of the Postgres-ordered result from the anchor — Postgres owns
// ordering, the Window only splices. complete=true means the result is
// exhausted at len(rows) (no deeper rows exist).
type Window struct {
	q        LiveQuery
	pred     match.Predicate
	n        int // visible size
	cushion  int // extra rows kept beyond N
	rows     []Row
	complete bool
	refill   Refiller
}

// NewWindow seeds a window from a snapshot prefix (visible+cushion already
// ordered). complete reports whether the snapshot exhausted the result.
func NewWindow(q LiveQuery, pred match.Predicate, seed []Row, complete bool, cushion int, refill Refiller) (*Window, error) {
	rows := make([]Row, len(seed))
	copy(rows, seed)
	return &Window{q: q, pred: pred, n: q.Limit, cushion: cushion, rows: rows, complete: complete, refill: refill}, nil
}

func (w *Window) cap() int { return w.n + w.cushion }

// indexOf returns the position of aggID in rows, or -1.
func (w *Window) indexOf(aggID string) int {
	for i := range w.rows {
		if w.rows[i].AggID == aggID {
			return i
		}
	}
	return -1
}

// insertPos returns where cur belongs in the ordered rows: the first row that
// sorts strictly after cur.
func (w *Window) insertPos(cur Cursor) int {
	return sort.Search(len(w.rows), func(i int) bool {
		return CompareCursors(w.rows[i].Cursor, cur, w.q.Sort) > 0
	})
}

// Apply folds one change into the window and returns the resulting deltas.
func (w *Window) Apply(ctx context.Context, ch Change) []LiveDelta {
	old := w.indexOf(ch.AggID)
	matches := !ch.Deleted && w.pred.Eval(ch.Vals)

	switch {
	case old < 0 && !matches:
		return nil // was out, still out
	case old < 0 && matches:
		return w.insert(ctx, ch)
	case old >= 0 && !matches:
		return w.remove(ctx, old, ch)
	default: // old >= 0 && matches
		return w.reposition(ctx, old, ch)
	}
}

func (w *Window) rowFromChange(ch Change) Row {
	return Row{
		AggID:   ch.AggID,
		Version: ch.Version,
		Cursor:  SortKeyOf(ch.Vals, w.q.Sort, ch.AggID),
		Raw:     ch.Raw,
		Vals:    ch.Vals,
	}
}

func (w *Window) insert(ctx context.Context, ch Change) []LiveDelta {
	r := w.rowFromChange(ch)
	pos := w.insertPos(r.Cursor)
	// Beyond the maintained prefix and the result isn't exhausted → not ours.
	if pos >= len(w.rows) && len(w.rows) >= w.cap() && !w.complete {
		return nil
	}
	// Capture the row at the visible boundary before splicing: an insert into
	// a full visible window pushes it into the cushion, so it must LEAVE.
	var evicted *Row
	if pos < w.n && len(w.rows) >= w.n {
		ev := w.rows[w.n-1]
		evicted = &ev
	}

	w.rows = append(w.rows, Row{})
	copy(w.rows[pos+1:], w.rows[pos:])
	w.rows[pos] = r

	var deltas []LiveDelta
	if pos < w.n {
		deltas = append(deltas, w.delta(OpEnter, r, -1, pos))
		if evicted != nil {
			deltas = append(deltas, w.delta(OpLeave, *evicted, w.n, -1))
		}
	}
	w.trimTail()
	deltas = append(deltas, w.maybeRefill(ctx)...)
	return deltas
}

func (w *Window) remove(ctx context.Context, pos int, ch Change) []LiveDelta {
	leaving := w.rows[pos]
	w.rows = append(w.rows[:pos], w.rows[pos+1:]...)

	var deltas []LiveDelta
	if pos < w.n {
		deltas = append(deltas, w.delta(OpLeave, leaving, pos, -1))
		// promote the first cushion row into the visible window, if present
		if w.n-1 < len(w.rows) {
			promoted := w.rows[w.n-1]
			deltas = append(deltas, w.delta(OpEnter, promoted, -1, w.n-1))
		}
	}
	deltas = append(deltas, w.maybeRefill(ctx)...)
	return deltas
}

func (w *Window) reposition(ctx context.Context, old int, ch Change) []LiveDelta {
	r := w.rowFromChange(ch)
	if CompareCursors(w.rows[old].Cursor, r.Cursor, w.q.Sort) == 0 {
		w.rows[old] = r // same position, payload-only change
		if old < w.n {
			return []LiveDelta{w.delta(OpUpdate, r, old, old)}
		}
		return nil
	}
	// remove then re-insert
	w.rows = append(w.rows[:old], w.rows[old+1:]...)
	pos := w.insertPos(r.Cursor)
	w.rows = append(w.rows, Row{})
	copy(w.rows[pos+1:], w.rows[pos:])
	w.rows[pos] = r

	var deltas []LiveDelta
	oldVisible := old < w.n
	newVisible := pos < w.n
	switch {
	case oldVisible && newVisible:
		deltas = append(deltas, w.delta(OpMove, r, old, pos))
	case oldVisible && !newVisible:
		deltas = append(deltas, w.delta(OpLeave, r, old, -1))
		if w.n-1 < len(w.rows) {
			deltas = append(deltas, w.delta(OpEnter, w.rows[w.n-1], -1, w.n-1))
		}
	case !oldVisible && newVisible:
		deltas = append(deltas, w.delta(OpEnter, r, -1, pos))
	}
	w.trimTail()
	deltas = append(deltas, w.maybeRefill(ctx)...)
	return deltas
}

// trimTail drops rows beyond the cushion cap (they belong to a deeper window).
func (w *Window) trimTail() {
	if len(w.rows) > w.cap() {
		w.rows = w.rows[:w.cap()]
		w.complete = false
	}
}

// maybeRefill tops the cushion back up from Postgres when it runs low, keeping
// the prefix exact. Refilled rows append at the tail (past the visible N), so
// no visible delta results.
func (w *Window) maybeRefill(ctx context.Context) []LiveDelta {
	low := w.cushion / 2
	if w.complete || len(w.rows) >= w.n+low {
		return nil
	}
	var after Cursor
	if len(w.rows) > 0 {
		after = w.rows[len(w.rows)-1].Cursor
	} else if w.q.Cursor != nil {
		after = *w.q.Cursor
	}
	need := w.cap() - len(w.rows)
	more, err := w.refill.After(ctx, w.q, after, need)
	if err != nil {
		return nil // suspicion flag for reconcile (P4); never corrupt the window
	}
	if len(more) < need {
		w.complete = true
	}
	w.rows = append(w.rows, more...)
	return nil
}

func (w *Window) delta(op DeltaOp, r Row, oldIdx, newIdx int) LiveDelta {
	return LiveDelta{
		Op:       op,
		AggID:    r.AggID,
		Version:  r.Version,
		Row:      r.Raw,
		OldIndex: oldIdx,
		NewIndex: newIdx,
		Cursor:   r.Cursor,
	}
}

// Visible returns the current visible window rows (for tests / re-snapshot).
func (w *Window) Visible() []Row {
	if len(w.rows) < w.n {
		return w.rows
	}
	return w.rows[:w.n]
}
