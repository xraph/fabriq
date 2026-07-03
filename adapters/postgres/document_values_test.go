package postgres

import (
	"encoding/json"
	"testing"

	"github.com/xraph/grove/crdt"
)

func hlcAt(ts int64, node string) crdt.HLC {
	return crdt.HLC{Timestamp: ts, NodeID: node}
}

// foldAll applies a sequence of change records like mergedState does.
func foldAll(t *testing.T, changes []crdt.ChangeRecord) *crdt.State {
	t.Helper()
	d := &DocStore{merge: crdt.NewMergeEngine()}
	state := crdt.NewState("fabriq_docs", "doc/1")
	for i := range changes {
		if err := d.fold(state, &changes[i]); err != nil {
			t.Fatalf("fold %d: %v", i, err)
		}
	}
	return state
}

func TestStateValues_AllTypesProject(t *testing.T) {
	txt := crdt.NewTextState()
	textOp, err := txt.Insert(crdt.TextRef{}, "hello", "a", hlcAt(30, "a"))
	if err != nil {
		t.Fatal(err)
	}

	state := foldAll(t, []crdt.ChangeRecord{
		{Field: "title", CRDTType: crdt.TypeLWW, HLC: hlcAt(10, "a"), NodeID: "a",
			Value: json.RawMessage(`"My Doc"`)},
		{Field: "views", CRDTType: crdt.TypeCounter, HLC: hlcAt(11, "a"), NodeID: "a",
			CounterDelta: &crdt.CounterDelta{Increment: 5}},
		{Field: "views", CRDTType: crdt.TypeCounter, HLC: hlcAt(12, "b"), NodeID: "b",
			CounterDelta: &crdt.CounterDelta{Increment: 2, Decrement: 1}},
		{Field: "tags", CRDTType: crdt.TypeSet, HLC: hlcAt(13, "a"), NodeID: "a",
			SetOp: &crdt.SetOperation{Op: crdt.SetOpAdd, Elements: json.RawMessage(`["x","y"]`)}},
		{Field: "steps", CRDTType: crdt.TypeList, HLC: hlcAt(14, "a"), NodeID: "a",
			ListOp: &crdt.ListOp{Op: crdt.ListOpInsert, NodeID: hlcAt(14, "a"), Value: json.RawMessage(`"one"`)}},
		{Field: "meta", CRDTType: crdt.TypeDocument, HLC: hlcAt(15, "a"), NodeID: "a",
			Value: json.RawMessage(`{"path":"author","value":"rex"}`)},
		{Field: "body", CRDTType: crdt.TypeText, HLC: hlcAt(30, "a"), NodeID: "a",
			TextOp: textOp},
	})

	vals := stateValues(state)

	if vals["title"] != "My Doc" {
		t.Fatalf("title = %v", vals["title"])
	}
	if vals["views"] != int64(6) { // 5 + 2 - 1
		t.Fatalf("views = %v (%T)", vals["views"], vals["views"])
	}
	tags, ok := vals["tags"].([]any)
	if !ok || len(tags) != 2 {
		t.Fatalf("tags = %v", vals["tags"])
	}
	steps, ok := vals["steps"].([]any)
	if !ok || len(steps) != 1 || steps[0] != "one" {
		t.Fatalf("steps = %v", vals["steps"])
	}
	meta, ok := vals["meta"].(map[string]any)
	if !ok || meta["author"] != "rex" {
		t.Fatalf("meta = %v", vals["meta"])
	}
	if vals["body"] != "hello" {
		t.Fatalf("body = %v", vals["body"])
	}
}

func TestFold_SetRemoveNoLongerLossy(t *testing.T) {
	// Regression: the old fold dropped SetOp payloads entirely, so a set
	// field would never contain its elements, let alone process removes.
	state := foldAll(t, []crdt.ChangeRecord{
		{Field: "tags", CRDTType: crdt.TypeSet, HLC: hlcAt(10, "a"), NodeID: "a",
			SetOp: &crdt.SetOperation{Op: crdt.SetOpAdd, Elements: json.RawMessage(`["x","y"]`)}},
		{Field: "tags", CRDTType: crdt.TypeSet, HLC: hlcAt(20, "b"), NodeID: "b",
			SetOp: &crdt.SetOperation{
				Op:       crdt.SetOpRemove,
				Elements: json.RawMessage(`["x"]`),
				Tags:     []crdt.Tag{{NodeID: "a", HLC: hlcAt(10, "a")}},
			}},
	})
	tags, _ := stateValues(state)["tags"].([]any)
	if len(tags) != 1 || tags[0] != "y" {
		t.Fatalf("tags = %v", tags)
	}
}

func TestFold_JSONRoundTripSnapshot(t *testing.T) {
	// Snapshots persist crdt.State as JSON; rich-type states must survive.
	txt := crdt.NewTextState()
	textOp, err := txt.Insert(crdt.TextRef{}, "persisted", "a", hlcAt(1, "a"))
	if err != nil {
		t.Fatal(err)
	}
	state := foldAll(t, []crdt.ChangeRecord{
		{Field: "body", CRDTType: crdt.TypeText, HLC: hlcAt(1, "a"), NodeID: "a", TextOp: textOp},
		{Field: "views", CRDTType: crdt.TypeCounter, HLC: hlcAt(2, "a"), NodeID: "a",
			CounterDelta: &crdt.CounterDelta{Increment: 3}},
	})
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	back := crdt.NewState("fabriq_docs", "doc/1")
	if err := json.Unmarshal(raw, back); err != nil {
		t.Fatal(err)
	}
	vals := stateValues(back)
	if vals["body"] != "persisted" || vals["views"] != int64(3) {
		t.Fatalf("round-trip vals = %v", vals)
	}
}
