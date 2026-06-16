package livequery_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
)

// driftSnap is a snapshot oracle whose truth can be changed between calls, to
// simulate the in-engine window diverging from Postgres.
type driftSnap struct {
	mu   sync.Mutex
	rows []livequery.Row
}

func (d *driftSnap) Snapshot(_ context.Context, _ livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.rows
	if len(r) > limit {
		r = r[:limit]
	}
	out := make([]livequery.Row, len(r))
	copy(out, r)
	return out, nil
}

func (d *driftSnap) set(rows []livequery.Row) {
	d.mu.Lock()
	d.rows = rows
	d.mu.Unlock()
}

func TestEngine_ReconcileRepairsDrift(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	mk := func(id, name string, v int64) livequery.Row {
		r := row(id, name, 1)
		r.Version = v
		r.Cursor = livequery.SortKeyOf(r.Vals, sortKeys, id)
		return r
	}
	snap := &driftSnap{rows: []livequery.Row{mk("a", "Alpha", 1)}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 8)}
	eng := livequery.NewEngine(snap, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 2, Buffer: 8})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 5, Where: query.Where{query.Eq("status", "active")}}

	snapshot, deltas, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if len(snapshot.Rows) != 1 || snapshot.Rows[0].AggID != "a" {
		t.Fatalf("initial snapshot = %+v, want [a]", snapshot.Rows)
	}

	// Truth diverges from the engine window (a gone, b appeared).
	snap.set([]livequery.Row{mk("b", "Bravo", 1)})
	n, err := eng.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reconcile repaired = %d, want 1", n)
	}
	if d := recvDelta(t, deltas); d.Op != livequery.OpReset {
		t.Fatalf("want OpReset on drift, got op=%v", d.Op)
	}

	// Now the window matches truth → a second reconcile repairs nothing.
	n, err = eng.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second reconcile repaired = %d, want 0", n)
	}
}
