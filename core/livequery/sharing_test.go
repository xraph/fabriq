package livequery_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
)

// countingSnap records how many times the oracle is consulted.
type countingSnap struct {
	mu    sync.Mutex
	rows  []livequery.Row
	calls int
}

func (c *countingSnap) Snapshot(_ context.Context, _ livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	r := c.rows
	if len(r) > limit {
		r = r[:limit]
	}
	out := make([]livequery.Row, len(r))
	copy(out, r)
	return out, nil
}

func (c *countingSnap) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// TestDispatcher_TemplatedSharing proves two subscriptions with the same query
// shape share one view: the snapshot oracle is consulted once, and one change
// fans out to both subscribers.
func TestDispatcher_TemplatedSharing(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 16)}
	snap := &countingSnap{}
	eng := livequery.NewEngine(snap, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 2, Buffer: 16})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 10, Where: query.Where{query.Eq("status", "active")}}

	_, d1, h1, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	_, d2, h2, err := eng.Subscribe(context.Background(), q) // identical shape → shares the view
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	if snap.count() != 1 {
		t.Fatalf("snapshot consulted %d times, want 1 (second subscriber shares the view)", snap.count())
	}

	pushChange(feed, "x", "Xray", "active", "valve", 2)
	assertEnter(t, d1, "x")
	assertEnter(t, d2, "x")
}

// cursorSnap returns a different page depending on whether a cursor is set, so
// a reanchor (which sets the cursor) sees a different result.
type cursorSnap struct {
	head, next []livequery.Row
}

func (c *cursorSnap) Snapshot(_ context.Context, q livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	rows := c.head
	if q.Cursor != nil {
		rows = c.next
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]livequery.Row, len(rows))
	copy(out, rows)
	return out, nil
}

func TestEngine_ReanchorSlidesWindow(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	mk := func(id, name string) livequery.Row {
		r := row(id, name, 1)
		r.Cursor = livequery.SortKeyOf(r.Vals, sortKeys, id)
		return r
	}
	snap := &cursorSnap{
		head: []livequery.Row{mk("a", "Alpha"), mk("b", "Bravo")},
		next: []livequery.Row{mk("c", "Charlie"), mk("d", "Delta")},
	}
	feed := &fakeFeed{ch: make(chan livequery.Change, 8)}
	eng := livequery.NewEngine(snap, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 2, Buffer: 8})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 2, Where: query.Where{query.Eq("status", "active")}}

	first, _, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if len(first.Rows) != 2 || first.Rows[0].AggID != "a" || first.Rows[1].AggID != "b" {
		t.Fatalf("head page = %+v, want [a b]", first.Rows)
	}

	// Scroll: re-anchor to the next page.
	cur := &livequery.Cursor{Values: []any{"Bravo", "b"}}
	second, err := h.Reanchor(context.Background(), cur, 2)
	if err != nil {
		t.Fatalf("reanchor: %v", err)
	}
	if len(second.Rows) != 2 || second.Rows[0].AggID != "c" || second.Rows[1].AggID != "d" {
		t.Fatalf("next page = %+v, want [c d]", second.Rows)
	}
}
