package livequery

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

// WHITE-BOX, and only for what has no behavioural signature.
//
// The streamed baseline is released on unmatch to stop a churning view growing
// without bound. That release cannot be seen from outside: a row that matches
// again immediately re-seeds its baseline, so every observable delta is
// identical whether or not the old one was ever dropped. The leak is real and
// the guard is real, so it is asserted against the map itself rather than
// deleted for being untestable from the outside.

func streamedView(t *testing.T, fieldDeltas bool) *view {
	t.Helper()
	pred, err := match.Compile(query.Where{query.Eq("status", "active")})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	q := LiveQuery{
		Entity: "asset", Limit: 5, Mode: ModeStreamed,
		Where:       query.Where{query.Eq("status", "active")},
		FieldDeltas: fieldDeltas,
	}
	return newView("v1", q, pred)
}

func vchange(id, status string, version int64) Change {
	vals := map[string]any{"id": id, "name": "n", "status": status}
	raw, _ := json.Marshal(vals)
	return Change{AggID: id, Version: version, Vals: vals, Raw: raw}
}

func TestFieldDelta_StreamedBaselineIsReleasedOnUnmatch(t *testing.T) {
	v := streamedView(t, true)

	v.applyStreamed(vchange("a", "active", 1)) // match → baseline held
	v.applyStreamed(vchange("b", "active", 1))
	if got := len(v.streamVals); got != 2 {
		t.Fatalf("held baselines = %d, want 2", got)
	}

	v.applyStreamed(vchange("a", "idle", 2)) // unmatch → baseline released
	if got := len(v.streamVals); got != 1 {
		t.Errorf("held baselines = %d after an unmatch, want 1: a view that "+
			"keeps them past the membership grows without bound as rows churn", got)
	}
	if _, still := v.streamVals["a"]; still {
		t.Error("the unmatched row's baseline is still held")
	}
}

/*
GUARDS the memory claim the opt-in flag exists to make. Without the flag a
streamed view must remain an ID-set: membership and nothing else, however many
updates pass through it.
*/
func TestFieldDelta_StreamedKeepsNoBaselineUnlessAsked(t *testing.T) {
	v := streamedView(t, false)

	v.applyStreamed(vchange("a", "active", 1))
	v.applyStreamed(vchange("a", "active", 2))
	v.applyStreamed(vchange("b", "active", 1))

	if v.streamVals != nil {
		t.Errorf("streamVals = %#v; a view nobody asked must allocate none", v.streamVals)
	}
	if len(v.streamMembers) != 2 {
		t.Errorf("membership = %d, want 2 — the ID-set itself must still work", len(v.streamMembers))
	}
}
