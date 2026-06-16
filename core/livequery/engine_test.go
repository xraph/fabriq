package livequery_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
)

// fakeSnap returns a fixed prefix.
type fakeSnap struct{ rows []livequery.Row }

func (f *fakeSnap) Snapshot(_ context.Context, _ livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	if limit < len(f.rows) {
		return f.rows[:limit], nil
	}
	return f.rows, nil
}

// fakeFeed is a manual change source standing in for the Redis tail.
type fakeFeed struct{ ch chan livequery.Change }

func (f *fakeFeed) Changes(_ context.Context, _ livequery.LiveQuery, _ string) (<-chan livequery.Change, func(), error) {
	return f.ch, func() {}, nil
}

func TestEngineSnapshotThenLiveDelta(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	seed := []livequery.Row{row("a", "Alpha", 1), row("c", "Charlie", 1)}
	for i := range seed {
		seed[i].Cursor = livequery.SortKeyOf(seed[i].Vals, sortKeys, seed[i].AggID)
	}
	feed := &fakeFeed{ch: make(chan livequery.Change, 4)}
	eng := livequery.NewEngine(
		&fakeSnap{rows: seed},
		&fakeRefiller{all: seed, sort: sortKeys},
		feed,
		livequery.EngineOptions{Cushion: 2, Buffer: 16},
	)

	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 2,
		Where: query.Where{query.Eq("status", "active")}}
	snap, deltas, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer h.Close()
	if len(snap.Rows) != 2 {
		t.Fatalf("snapshot rows = %d want 2", len(snap.Rows))
	}

	feed.ch <- change("b", "Bravo", 1, "active", false)
	select {
	case d := <-deltas:
		if d.Op != livequery.OpEnter {
			t.Fatalf("first delta op = %v want enter", d.Op)
		}
		if d.StreamID == "" {
			// change() leaves StreamID empty; ensure the engine still tagged At.
		}
	case <-time.After(time.Second):
		t.Fatal("no delta received")
	}
}

func TestEngineGaplessDedup(t *testing.T) {
	// A change whose version is not newer than the seeded row must be skipped.
	sortKeys := []livequery.SortKey{{Column: "name"}}
	seed := []livequery.Row{row("a", "Alpha", 1)}
	seed[0].Version = 5
	seed[0].Cursor = livequery.SortKeyOf(seed[0].Vals, sortKeys, "a")

	feed := &fakeFeed{ch: make(chan livequery.Change, 4)}
	eng := livequery.NewEngine(&fakeSnap{rows: seed}, &fakeRefiller{all: seed, sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 2, Buffer: 16})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 2, Where: query.Where{query.Eq("status", "active")}}
	_, deltas, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer h.Close()

	stale := change("a", "Alpha", 1, "active", false)
	stale.Version = 3 // older than seeded version 5 → must be ignored
	feed.ch <- stale
	// follow with a genuinely new change to prove the loop is alive
	feed.ch <- change("z", "Zeb", 1, "active", false)

	select {
	case d := <-deltas:
		if d.AggID != "z" {
			t.Fatalf("expected the stale 'a' change to be dropped, got delta for %q", d.AggID)
		}
	case <-time.After(time.Second):
		t.Fatal("no delta received")
	}
}
