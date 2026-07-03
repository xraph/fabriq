package document_test

// The document-plane CONTRACT suite: behaviors every document.Store must
// exhibit, run against fabriqtest.FakeDocumentStore (the in-memory
// implementation). The postgres adapter proves the same behaviors in its
// Docker-gated integration suite (adapters/postgres/document_*_test.go).
//
// Two sketched phase-7 behaviors live elsewhere by design:
//   - quiet-window materialization emitting ONE versioned event is an
//     adapter+worker concern (TestDocMaterialize_* + worker tests);
//   - conflation bypass is a transport concern (core/subscribe raw-path
//     tests + the gateway document endpoints).

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/fabriqtest"
)

func hlcAt(ts int64, node string) crdt.HLC {
	return crdt.HLC{Timestamp: ts, NodeID: node}
}

func encodeUpdate(t *testing.T, changes ...crdt.ChangeRecord) []byte {
	t.Helper()
	raw, err := json.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func vector(seq int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(seq)) // #nosec G115 -- test seqs are tiny
	return out
}

type syncReply struct {
	Seq      int64             `json:"seq"`
	Snapshot json.RawMessage   `json:"snapshot,omitempty"`
	Updates  []json.RawMessage `json:"updates"`
}

func doSync(t *testing.T, store document.Store, docID string, v []byte) syncReply {
	t.Helper()
	raw, err := store.Sync(context.Background(), docID, v)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	var reply syncReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("sync reply: %v", err)
	}
	return reply
}

func lwwUpdate(t *testing.T, field, value string, ts int64) []byte {
	t.Helper()
	return encodeUpdate(t, crdt.ChangeRecord{
		Field: field, CRDTType: crdt.TypeLWW,
		HLC: hlcAt(ts, "a"), NodeID: "a",
		Value: json.RawMessage(`"` + value + `"`),
	})
}

func TestApplyUpdate_AppendsToLog(t *testing.T) {
	store := &fabriqtest.FakeDocumentStore{}
	ctx := context.Background()
	docID := "note/01"

	if err := store.ApplyUpdate(ctx, docID, lwwUpdate(t, "title", "one", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyUpdate(ctx, docID, lwwUpdate(t, "title", "two", 2)); err != nil {
		t.Fatal(err)
	}
	// Garbage is rejected.
	if err := store.ApplyUpdate(ctx, docID, []byte(`{}`)); err == nil {
		t.Fatal("non-array update must be rejected")
	}
	if err := store.ApplyUpdate(ctx, docID, []byte(`[]`)); err == nil {
		t.Fatal("empty update must be rejected")
	}

	reply := doSync(t, store, docID, nil)
	if len(reply.Updates) != 2 {
		t.Fatalf("updates = %d, want 2", len(reply.Updates))
	}
	if reply.Seq != 2 {
		t.Fatalf("vector seq = %d, want 2", reply.Seq)
	}
}

func TestSync_ReturnsMissingUpdates(t *testing.T) {
	store := &fabriqtest.FakeDocumentStore{}
	ctx := context.Background()
	docID := "note/01"
	for i := int64(1); i <= 3; i++ {
		if err := store.ApplyUpdate(ctx, docID, lwwUpdate(t, "title", "v", i)); err != nil {
			t.Fatal(err)
		}
	}

	// A client at seq 1 receives exactly updates 2 and 3.
	reply := doSync(t, store, docID, vector(1))
	if len(reply.Updates) != 2 || reply.Seq != 3 {
		t.Fatalf("reply = %+v", reply)
	}
	// A caught-up client receives nothing.
	reply = doSync(t, store, docID, vector(3))
	if len(reply.Updates) != 0 || reply.Seq != 3 {
		t.Fatalf("caught-up reply = %+v", reply)
	}
}

func TestCompact_FoldsLogIntoSnapshot(t *testing.T) {
	store := &fabriqtest.FakeDocumentStore{}
	ctx := context.Background()
	docID := "note/01"

	txt := crdt.NewTextState()
	op, err := txt.Insert(crdt.TextRef{}, "hello", "a", hlcAt(5, "a"))
	if err != nil {
		t.Fatal(err)
	}
	updates := [][]byte{
		lwwUpdate(t, "title", "doc", 1),
		encodeUpdate(t, crdt.ChangeRecord{
			Field: "views", CRDTType: crdt.TypeCounter, HLC: hlcAt(2, "a"), NodeID: "a",
			CounterDelta: &crdt.CounterDelta{Increment: 4},
		}),
		encodeUpdate(t, crdt.ChangeRecord{
			Field: "body", CRDTType: crdt.TypeText, HLC: hlcAt(5, "a"), NodeID: "a", TextOp: op,
		}),
	}
	for _, u := range updates {
		if err := store.ApplyUpdate(ctx, docID, u); err != nil {
			t.Fatal(err)
		}
	}

	before, err := store.Snapshot(ctx, docID)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Compact(ctx, docID); err != nil {
		t.Fatal(err)
	}

	// Compaction changes storage shape, never merge results.
	after, err := store.Snapshot(ctx, docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(before.Snapshot) != string(after.Snapshot) {
		t.Fatalf("snapshot changed by compaction:\n%s\n%s", before.Snapshot, after.Snapshot)
	}
	var vals map[string]any
	if err := json.Unmarshal(after.Snapshot, &vals); err != nil {
		t.Fatal(err)
	}
	if vals["title"] != "doc" || vals["views"] != float64(4) || vals["body"] != "hello" {
		t.Fatalf("vals = %v", vals)
	}

	// A from-scratch client now heals via snapshot + empty tail...
	reply := doSync(t, store, docID, nil)
	if len(reply.Snapshot) == 0 {
		t.Fatal("post-compaction sync must carry the snapshot")
	}
	if len(reply.Updates) != 0 || reply.Seq != 3 {
		t.Fatalf("post-compaction reply = %+v", reply)
	}
	// ...and post-compaction updates ride the tail as usual.
	if err := store.ApplyUpdate(ctx, docID, lwwUpdate(t, "title", "doc2", 9)); err != nil {
		t.Fatal(err)
	}
	reply = doSync(t, store, docID, vector(3))
	if len(reply.Snapshot) != 0 || len(reply.Updates) != 1 || reply.Seq != 4 {
		t.Fatalf("tail reply = %+v", reply)
	}
}

func TestSnapshot_RichTypesProject(t *testing.T) {
	store := &fabriqtest.FakeDocumentStore{}
	ctx := context.Background()
	docID := "note/01"

	if err := store.ApplyUpdate(ctx, docID, encodeUpdate(t,
		crdt.ChangeRecord{
			Field: "tags", CRDTType: crdt.TypeSet, HLC: hlcAt(1, "a"), NodeID: "a",
			SetOp: &crdt.SetOperation{Op: crdt.SetOpAdd, Elements: json.RawMessage(`["x","y"]`)},
		},
		crdt.ChangeRecord{
			Field: "tags", CRDTType: crdt.TypeSet, HLC: hlcAt(2, "b"), NodeID: "b",
			SetOp: &crdt.SetOperation{
				Op: crdt.SetOpRemove, Elements: json.RawMessage(`["x"]`),
				Tags: []crdt.Tag{{NodeID: "a", HLC: hlcAt(1, "a")}},
			},
		},
	)); err != nil {
		t.Fatal(err)
	}

	mat, err := store.Snapshot(ctx, docID)
	if err != nil {
		t.Fatal(err)
	}
	var vals map[string]any
	if err := json.Unmarshal(mat.Snapshot, &vals); err != nil {
		t.Fatal(err)
	}
	tags, ok := vals["tags"].([]any)
	if !ok || len(tags) != 1 || tags[0] != "y" {
		t.Fatalf("tags = %v", vals["tags"])
	}
}
