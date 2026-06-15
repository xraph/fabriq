package livequery_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

// fakeRefiller serves keyset rows from an in-memory ordered set.
type fakeRefiller struct {
	all  []livequery.Row
	sort []livequery.SortKey
}

func (f *fakeRefiller) After(_ context.Context, _ livequery.LiveQuery, after livequery.Cursor, limit int) ([]livequery.Row, error) {
	var out []livequery.Row
	for _, r := range f.all {
		if livequery.CompareCursors(r.Cursor, after, f.sort) > 0 {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func row(id, name string, temp float64) livequery.Row {
	vals := map[string]any{"id": id, "name": name, "temp": temp, "status": "active"}
	raw, _ := json.Marshal(vals)
	return livequery.Row{AggID: id, Version: 1, Vals: vals, Raw: raw}
}

func newWindow(t *testing.T, n int, seed []livequery.Row, all []livequery.Row) *livequery.Window {
	t.Helper()
	sortKeys := []livequery.SortKey{{Column: "name"}}
	for i := range seed {
		seed[i].Cursor = livequery.SortKeyOf(seed[i].Vals, sortKeys, seed[i].AggID)
	}
	for i := range all {
		all[i].Cursor = livequery.SortKeyOf(all[i].Vals, sortKeys, all[i].AggID)
	}
	pred, _ := match.Compile(query.Where{query.Eq("status", "active")})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: n,
		Where: query.Where{query.Eq("status", "active")}}
	w, err := livequery.NewWindow(q, pred, seed, false, 2, &fakeRefiller{all: all, sort: sortKeys})
	if err != nil {
		t.Fatalf("NewWindow: %v", err)
	}
	return w
}

func change(id, name string, temp float64, status string, deleted bool) livequery.Change {
	vals := map[string]any{"id": id, "name": name, "temp": temp, "status": status}
	raw, _ := json.Marshal(vals)
	c := livequery.Change{AggID: id, Version: 2, Raw: raw, Deleted: deleted}
	if !deleted {
		c.Vals = vals
	}
	return c
}

func TestWindowEnterMoveLeaveUpdate(t *testing.T) {
	// window N=2 over [Alpha, Charlie]; deeper rows [Echo, Golf] for refill.
	seed := []livequery.Row{row("a", "Alpha", 1), row("c", "Charlie", 1)}
	all := []livequery.Row{row("a", "Alpha", 1), row("c", "Charlie", 1), row("e", "Echo", 1), row("g", "Golf", 1)}
	w := newWindow(t, 2, seed, all)

	// ENTER: new "Bravo" sorts to index 1 (between Alpha and Charlie).
	d := w.Apply(context.Background(), change("b", "Bravo", 1, "active", false))
	if len(d) == 0 || d[0].Op != livequery.OpEnter || d[0].NewIndex != 1 {
		t.Fatalf("expected enter at 1, got %+v", d)
	}

	// UPDATE: Alpha payload changes, same position.
	d = w.Apply(context.Background(), change("a", "Alpha", 99, "active", false))
	if len(d) != 1 || d[0].Op != livequery.OpUpdate || d[0].NewIndex != 0 {
		t.Fatalf("expected update at 0, got %+v", d)
	}

	// LEAVE via predicate: Bravo goes inactive → leaves; cushion refills the slot.
	d = w.Apply(context.Background(), change("b", "Bravo", 1, "idle", false))
	var sawLeave, sawEnter bool
	for _, x := range d {
		if x.Op == livequery.OpLeave && x.AggID == "b" {
			sawLeave = true
		}
		if x.Op == livequery.OpEnter {
			sawEnter = true // a cushion/refill row becomes visible
		}
	}
	if !sawLeave || !sawEnter {
		t.Fatalf("expected leave(b)+enter(refill), got %+v", d)
	}

	// MOVE: the index-1 visible row (Charlie) is renamed but stays visible.
	d = w.Apply(context.Background(), change("c", "Bravo", 1, "active", false))
	var sawMove bool
	for _, x := range d {
		if x.Op == livequery.OpMove && x.AggID == "c" {
			sawMove = true
		}
	}
	if !sawMove {
		t.Fatalf("expected move(c), got %+v", d)
	}
}
