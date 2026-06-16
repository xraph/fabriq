package livequery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
)

func pushChange(feed *fakeFeed, id, name, status, kind string, version int64) {
	vals := map[string]any{"id": id, "name": name, "status": status, "kind": kind}
	raw, _ := json.Marshal(vals)
	feed.ch <- livequery.Change{AggID: id, Version: version, Vals: vals, Raw: raw}
}

func assertEnter(t *testing.T, ch <-chan livequery.LiveDelta, aggID string) {
	t.Helper()
	select {
	case d := <-ch:
		if d.Op != livequery.OpEnter || d.AggID != aggID {
			t.Fatalf("want enter(%s), got op=%v agg=%s", aggID, d.Op, d.AggID)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no enter delta for %s", aggID)
	}
}

func assertNoDelta(t *testing.T, ch <-chan livequery.LiveDelta) {
	t.Helper()
	select {
	case d := <-ch:
		t.Fatalf("unexpected delta: op=%v agg=%s", d.Op, d.AggID)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestDispatcher_IndexRoutesToMatchingSubsOnly proves two subscriptions sharing
// one partition each receive only the changes their filter matches — the
// predicate index routes, instead of every sub seeing every change.
func TestDispatcher_IndexRoutesToMatchingSubsOnly(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 16)}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 4, Buffer: 16})

	qA := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 10, Where: query.Where{query.Eq("status", "active")}}
	qB := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 10, Where: query.Where{query.Eq("kind", "pump")}}

	_, dA, cancelA, err := eng.Subscribe(context.Background(), qA)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelA()
	_, dB, cancelB, err := eng.Subscribe(context.Background(), qB)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelB()

	// matches A only (active, not pump)
	pushChange(feed, "x", "Xray", "active", "valve", 2)
	assertEnter(t, dA, "x")
	assertNoDelta(t, dB)

	// matches B only (pump, not active)
	pushChange(feed, "y", "Yankee", "idle", "pump", 2)
	assertEnter(t, dB, "y")
	assertNoDelta(t, dA)

	// matches both
	pushChange(feed, "z", "Zulu", "active", "pump", 2)
	assertEnter(t, dA, "z")
	assertEnter(t, dB, "z")
}

type fakeMembers struct{ ids []string }

func (f *fakeMembers) Members(_ context.Context, _ livequery.LiveQuery) ([]string, error) {
	return f.ids, nil
}

func recvDelta(t *testing.T, ch <-chan livequery.LiveDelta) livequery.LiveDelta {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("no delta received")
		return livequery.LiveDelta{}
	}
}

// TestDispatcher_StreamedMatchUnmatchUpdate exercises Streamed mode: a seeded
// member that unmatches, a fresh row that matches, and an in-set update.
func TestDispatcher_StreamedMatchUnmatchUpdate(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 16)}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Buffer: 16, Members: &fakeMembers{ids: []string{"seed1"}}})
	q := livequery.LiveQuery{
		Entity: "asset", Sort: sortKeys, Limit: 5, Mode: livequery.ModeStreamed,
		Where: query.Where{query.Eq("status", "active")},
	}
	_, deltas, cancel, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	// A fresh row that matches → OpMatch.
	pushChange(feed, "m", "Mike", "active", "pump", 2)
	if d := recvDelta(t, deltas); d.Op != livequery.OpMatch || d.AggID != "m" {
		t.Fatalf("want match(m), got op=%v agg=%s", d.Op, d.AggID)
	}

	// A seeded member that stops matching → OpUnmatch (caught via the member index).
	pushChange(feed, "seed1", "Seed", "idle", "pump", 2)
	if d := recvDelta(t, deltas); d.Op != livequery.OpUnmatch || d.AggID != "seed1" {
		t.Fatalf("want unmatch(seed1), got op=%v agg=%s", d.Op, d.AggID)
	}

	// An in-set update → OpUpdate.
	pushChange(feed, "m", "Mike2", "active", "pump", 3)
	if d := recvDelta(t, deltas); d.Op != livequery.OpUpdate || d.AggID != "m" {
		t.Fatalf("want update(m), got op=%v agg=%s", d.Op, d.AggID)
	}
}

// TestDispatcher_ConcurrentSubscribeCancel stresses register/activate/
// deregister against a live dispatch loop — run under -race.
func TestDispatcher_ConcurrentSubscribeCancel(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 256)}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Cushion: 4, Buffer: 32})
	q := livequery.LiveQuery{Entity: "asset", Sort: sortKeys, Limit: 10, Where: query.Where{query.Eq("status", "active")}}

	// A writer goroutine pushes changes throughout.
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			pushChange(feed, fmt.Sprintf("a%d", i%50), fmt.Sprintf("n%d", i%50), "active", "pump", int64(i))
		}
	}()

	var subs sync.WaitGroup
	for i := 0; i < 40; i++ {
		subs.Add(1)
		go func() {
			defer subs.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, deltas, release, err := eng.Subscribe(ctx, q)
			if err != nil {
				t.Errorf("subscribe: %v", err)
				return
			}
			// drain a few deltas, then leave.
			deadline := time.After(200 * time.Millisecond)
			for {
				select {
				case <-deltas:
				case <-deadline:
					release()
					return
				}
			}
		}()
	}
	subs.Wait()
	close(stop)
	writer.Wait()
}
