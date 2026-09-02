package livequery_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

// fieldWindow is `newWindow` with field deltas asked for, so the two can be
// compared: the ONLY difference between them is the flag.
func fieldWindow(t *testing.T, n int, seed []livequery.Row, all []livequery.Row, fieldDeltas bool) *livequery.Window {
	t.Helper()
	sortKeys := []livequery.SortKey{{Column: "name"}}
	for i := range seed {
		seed[i].Cursor = livequery.SortKeyOf(seed[i].Vals, sortKeys, seed[i].AggID)
	}
	for i := range all {
		all[i].Cursor = livequery.SortKeyOf(all[i].Vals, sortKeys, all[i].AggID)
	}
	pred, _ := match.Compile(query.Where{query.Eq("status", "active")})
	q := livequery.LiveQuery{
		Entity: "asset", Sort: sortKeys, Limit: n,
		Where:       query.Where{query.Eq("status", "active")},
		FieldDeltas: fieldDeltas,
	}
	w, err := livequery.NewWindow(q, pred, seed, false, 2, &fakeRefiller{all: all, sort: sortKeys})
	if err != nil {
		t.Fatalf("NewWindow: %v", err)
	}
	return w
}

// rowOf builds a Row from an explicit column map, so a test can state the
// before-image it means rather than inheriting one.
func rowOf(vals map[string]any) livequery.Row {
	raw, _ := json.Marshal(vals)
	id, _ := vals["id"].(string)
	return livequery.Row{AggID: id, Version: 1, Vals: vals, Raw: raw}
}

func findOp(t *testing.T, deltas []livequery.LiveDelta, op livequery.DeltaOp) livequery.LiveDelta {
	t.Helper()
	for _, d := range deltas {
		if d.Op == op {
			return d
		}
	}
	t.Fatalf("no %v delta in %v", op, deltas)
	return livequery.LiveDelta{}
}

// ─── The differ itself ──────────────────────────────────────────────────────

func TestFieldDelta_Window_ReportsOnlyMovedFields(t *testing.T) {
	t.Parallel()

	// Each case states its OWN before-image rather than sharing the package's
	// `row` helper: the shared one carries the predicate column, so a case
	// that moved it made the row LEAVE the window and never produced an
	// update at all. Stating both halves keeps a case from silently testing a
	// different op than it is named for.
	base := map[string]any{"id": "r1", "name": "b", "temp": 10.0, "status": "active", "site": "Bonga"}

	tests := []struct {
		name string
		next map[string]any
		want map[string]any
	}{
		{
			name: "one field moved",
			next: map[string]any{"id": "r1", "name": "b", "temp": 11.0, "status": "active", "site": "Bonga"},
			want: map[string]any{"temp": 11.0},
		},
		{
			name: "several fields moved",
			next: map[string]any{"id": "r1", "name": "b", "temp": 11.0, "status": "active", "site": "Forcados"},
			want: map[string]any{"temp": 11.0, "site": "Forcados"},
		},
		{
			// An added field is a change: the client has no cell for it yet.
			name: "a field the row did not have before",
			next: map[string]any{"id": "r1", "name": "b", "temp": 10.0, "status": "active", "site": "Bonga", "unit": "FPSO-1"},
			want: map[string]any{"unit": "FPSO-1"},
		},
		{
			// nil rather than omission, so a client can tell "gone" from
			// "unchanged" instead of keeping a stale cell forever.
			name: "a field the row dropped comes back as nil",
			next: map[string]any{"id": "r1", "name": "b", "status": "active", "site": "Bonga"},
			want: map[string]any{"temp": nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := fieldWindow(t, 3, []livequery.Row{rowOf(base)}, nil, true)

			raw, _ := json.Marshal(tt.next)
			ch := livequery.Change{AggID: "r1", Version: 2, Raw: raw, Vals: tt.next}
			d := findOp(t, w.Apply(context.Background(), ch), livequery.OpUpdate)

			if !reflect.DeepEqual(d.Changes, tt.want) {
				t.Errorf("Changes = %#v, want %#v", d.Changes, tt.want)
			}
			// ADDITIVE: the whole row still ships, so a client that ignores
			// Changes keeps working exactly as before.
			if len(d.Row) == 0 {
				t.Error("Row must still carry the full row beside the delta")
			}
		})
	}
}

/*
GUARDS the opt-in. A subscription that did not ask for field deltas must get
none — the flag is what makes the streamed mode's memory cost a choice, and a
window that reported them anyway would make that flag look decorative.
*/
func TestFieldDelta_Window_SilentUnlessAsked(t *testing.T) {
	t.Parallel()

	for _, asked := range []bool{true, false} {
		name := "asked"
		if !asked {
			name = "not asked"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := fieldWindow(t, 3, []livequery.Row{row("r1", "b", 10)}, nil, asked)
			d := findOp(t, w.Apply(context.Background(), change("r1", "b", 99, "active")), livequery.OpUpdate)

			if asked && d.Changes == nil {
				t.Fatal("asked for field deltas and got none")
			}
			if !asked && d.Changes != nil {
				t.Fatalf("did not ask for field deltas, got %#v", d.Changes)
			}
		})
	}
}

/*
GUARDS the nil-vs-empty distinction. An empty non-nil map says "something
changed, but nothing you can see", which a client reads as work to do; nil says
nothing moved. `omitempty` hides both on the wire, so only the Go-side contract
can be asserted here.
*/
func TestFieldDelta_Window_NothingMovedYieldsNil(t *testing.T) {
	t.Parallel()
	w := fieldWindow(t, 3, []livequery.Row{row("r1", "b", 10)}, nil, true)

	// Same values, new version — the feed re-sent a row it had already sent.
	deltas := w.Apply(context.Background(), change("r1", "b", 10, "active"))
	d := findOp(t, deltas, livequery.OpUpdate)
	if d.Changes != nil {
		t.Errorf("Changes = %#v, want nil for a row that moved nothing", d.Changes)
	}
}

/*
A move is a payload change too — the row would not have re-sorted otherwise —
so it carries the same delta an in-place update would.
*/
func TestFieldDelta_Window_MoveCarriesTheDelta(t *testing.T) {
	t.Parallel()
	seed := []livequery.Row{row("r1", "b", 10), row("r2", "c", 20)}
	w := fieldWindow(t, 3, seed, nil, true)

	// `name` is the sort column, so changing it re-sorts the row.
	d := findOp(t, w.Apply(context.Background(), change("r1", "z", 10, "active")), livequery.OpMove)
	if want := map[string]any{"name": "z"}; !reflect.DeepEqual(d.Changes, want) {
		t.Errorf("Changes = %#v, want %#v", d.Changes, want)
	}
}

/*
GUARDS what a field delta is RELATIVE to. A row entering the window is new to
this client, so there is no before-image and a delta would be a claim about
state the client never held. Reported as one, an enter would flash every cell
of every arriving row.
*/
func TestFieldDelta_Window_EnterCarriesNoDelta(t *testing.T) {
	t.Parallel()
	w := fieldWindow(t, 3, []livequery.Row{row("r1", "b", 10)}, nil, true)

	d := findOp(t, w.Apply(context.Background(), change("r9", "a", 5, "active")), livequery.OpEnter)
	if d.Changes != nil {
		t.Errorf("an enter carried %#v; it has no previous state to differ from", d.Changes)
	}
}

// ─── Streamed mode, where the before-image is bought rather than borrowed ────

// streamedSub subscribes in Streamed mode with field deltas on or off. The two
// differ only in the flag, which is what makes the memory claim testable.
func streamedSub(t *testing.T, fieldDeltas bool) (*fakeFeed, <-chan livequery.LiveDelta) {
	t.Helper()
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 16)}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Buffer: 16})
	q := livequery.LiveQuery{
		Entity: "asset", Sort: sortKeys, Limit: 5, Mode: livequery.ModeStreamed,
		Where:       query.Where{query.Eq("status", "active")},
		FieldDeltas: fieldDeltas,
	}
	_, deltas, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return feed, deltas
}

/*
GUARDS the whole point of the streamed path: a view that keeps only an ID-set
has no before-image, so the baseline has to be REMEMBERED on the match for the
next update to be measurable against anything.
*/
func TestFieldDelta_Streamed_UpdateMeasuredAgainstRememberedRow(t *testing.T) {
	feed, deltas := streamedSub(t, true)

	// The match establishes the baseline and carries no delta itself: the
	// client had no previous state for this row.
	pushChange(feed, "m", "Mike", "active", "pump", 2)
	if d := recvDelta(t, deltas); d.Op != livequery.OpMatch || d.Changes != nil {
		t.Fatalf("match carried op=%v changes=%#v; want match with no delta", d.Op, d.Changes)
	}

	// Only `name` moved.
	pushChange(feed, "m", "Mike2", "active", "pump", 3)
	d := recvDelta(t, deltas)
	if d.Op != livequery.OpUpdate {
		t.Fatalf("op = %v, want update", d.Op)
	}
	if want := map[string]any{"name": "Mike2"}; !reflect.DeepEqual(d.Changes, want) {
		t.Errorf("Changes = %#v, want %#v", d.Changes, want)
	}
}

/*
GUARDS the opt-in where it actually costs something. Without the flag a
streamed view must stay an ID-set, so an update reports no field delta at all
and no baseline is retained to produce one.
*/
func TestFieldDelta_Streamed_SilentUnlessAsked(t *testing.T) {
	feed, deltas := streamedSub(t, false)

	pushChange(feed, "m", "Mike", "active", "pump", 2)
	_ = recvDelta(t, deltas) // match
	pushChange(feed, "m", "Mike2", "active", "pump", 3)

	d := recvDelta(t, deltas)
	if d.Op != livequery.OpUpdate {
		t.Fatalf("op = %v, want update", d.Op)
	}
	if d.Changes != nil {
		t.Errorf("Changes = %#v; a view that was not asked must keep no baseline", d.Changes)
	}
}

/*
GUARDS the release. The baseline is held for exactly as long as the membership:
kept past an unmatch it is a leak on a churning view, and a stale before-image
if the row ever matches again — which would report fields as moved that moved
while nobody was watching.
*/
func TestFieldDelta_Streamed_BaselineReleasedOnUnmatch(t *testing.T) {
	feed, deltas := streamedSub(t, true)

	pushChange(feed, "m", "Mike", "active", "pump", 2)
	_ = recvDelta(t, deltas) // match

	pushChange(feed, "m", "Mike", "idle", "pump", 3)
	if d := recvDelta(t, deltas); d.Op != livequery.OpUnmatch {
		t.Fatalf("op = %v, want unmatch", d.Op)
	}

	// It matches again, with a value that moved while it was out of the set.
	pushChange(feed, "m", "Mike9", "active", "pump", 4)
	d := recvDelta(t, deltas)
	if d.Op != livequery.OpMatch {
		t.Fatalf("op = %v, want match", d.Op)
	}
	if d.Changes != nil {
		t.Errorf("Changes = %#v; a re-match is an arrival, not a change", d.Changes)
	}
}

/*
GUARDS the nil baseline, on the path that really produces one.

A streamed view seeds its membership from the Members port (engine.go) — ids
only, no payloads — so the FIRST update to a seeded member has nothing to be
measured against. Reported as "every field changed", that update would flash a
whole row for every seeded member the moment the feed first touched it, which
is precisely the noise field deltas exist to remove.

No baseline is not the same as a total change. The full row still ships in
`Row`, so a client that needs it has it.
*/
func TestFieldDelta_Streamed_SeededMemberHasNoBaselineToDifferFrom(t *testing.T) {
	sortKeys := []livequery.SortKey{{Column: "name"}}
	feed := &fakeFeed{ch: make(chan livequery.Change, 16)}
	eng := livequery.NewEngine(&fakeSnap{}, &fakeRefiller{sort: sortKeys}, feed,
		livequery.EngineOptions{Buffer: 16, Members: &fakeMembers{ids: []string{"seed1"}}})
	q := livequery.LiveQuery{
		Entity: "asset", Sort: sortKeys, Limit: 5, Mode: livequery.ModeStreamed,
		Where:       query.Where{query.Eq("status", "active")},
		FieldDeltas: true,
	}
	_, deltas, h, err := eng.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// seed1 is a member with no remembered payload: it arrived as an id.
	pushChange(feed, "seed1", "Seed2", "active", "pump", 2)
	d := recvDelta(t, deltas)
	if d.Op != livequery.OpUpdate {
		t.Fatalf("op = %v, want update", d.Op)
	}
	if d.Changes != nil {
		t.Errorf("Changes = %#v; with no baseline the delta must be absent, "+
			"not a claim that every field moved", d.Changes)
	}
	if len(d.Row) == 0 {
		t.Error("the full row must still ship when no field delta is available")
	}
}
