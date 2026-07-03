//go:build integration

package postgres_test

// CompactDue is the worker-plane compaction trigger: documents whose
// un-compacted update count reached their entity's SnapshotEvery budget
// (64 for the demo "page" entity) get their log folded into the snapshot.
// Compact additionally GCs tombstones behind the safety horizon.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
)

func TestCompactDue_TriggersOnBudget(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents()

	tctx, _ := tenant.WithTenant(ctx, "t1")
	docID := "page/" + event.NewID()

	// One update short of the budget: not due.
	for i := 0; i < 63; i++ {
		if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", i, int64(i+1), "n1")); err != nil {
			t.Fatal(err)
		}
	}
	n, err := ds.CompactDue(ctx)
	if err != nil {
		t.Fatalf("CompactDue: %v", err)
	}
	if n != 0 {
		t.Fatalf("compacted %d docs below budget, want 0", n)
	}

	// Crossing the budget makes it due exactly once.
	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "final", 64, "n1")); err != nil {
		t.Fatal(err)
	}
	n, err = ds.CompactDue(ctx)
	if err != nil {
		t.Fatalf("CompactDue: %v", err)
	}
	if n != 1 {
		t.Fatalf("compacted %d docs, want 1", n)
	}
	n, err = ds.CompactDue(ctx)
	if err != nil {
		t.Fatalf("CompactDue (second): %v", err)
	}
	if n != 0 {
		t.Fatalf("second pass compacted %d docs, want 0", n)
	}

	// The snapshot took over: a fresh Sync carries it and no tail.
	raw, err := ds.Sync(tctx, docID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Seq      int64             `json:"seq"`
		Snapshot json.RawMessage   `json:"snapshot"`
		Updates  []json.RawMessage `json:"updates"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Snapshot) == 0 || len(payload.Updates) != 0 || payload.Seq != 64 {
		t.Fatalf("post-compaction sync: seq=%d snapshot=%dB updates=%d", payload.Seq, len(payload.Snapshot), len(payload.Updates))
	}
}

func TestCompact_GCsTombstonesBehindHorizon(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents()

	tctx, _ := tenant.WithTenant(ctx, "t1")
	docID := "page/" + event.NewID()

	// A list element inserted and deleted two hours ago (well behind the
	// 1h GC window), plus a fresh write that advances the horizon.
	old := time.Now().Add(-2 * time.Hour).UnixNano()
	nodeID := crdt.HLC{Timestamp: old, NodeID: "n1"}
	insert, err := json.Marshal([]crdt.ChangeRecord{{
		Table: "pages", PK: docID, Field: "blocks", CRDTType: crdt.TypeList,
		HLC: nodeID, NodeID: "n1",
		ListOp: &crdt.ListOp{Op: crdt.ListOpInsert, NodeID: nodeID, Value: json.RawMessage(`"ghost-block"`)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	del, err := json.Marshal([]crdt.ChangeRecord{{
		Table: "pages", PK: docID, Field: "blocks", CRDTType: crdt.TypeList,
		HLC: crdt.HLC{Timestamp: old + 1, NodeID: "n1"}, NodeID: "n1",
		ListOp: &crdt.ListOp{Op: crdt.ListOpDelete, NodeID: nodeID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range [][]byte{insert, del,
		crdtLWWUpdate(t, "pages", docID, "title", "fresh", time.Now().UnixNano(), "n1")} {
		if err := ds.ApplyUpdate(tctx, docID, u); err != nil {
			t.Fatal(err)
		}
	}

	if err := ds.Compact(tctx, docID); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// The tombstoned leaf is GC'd out of the persisted snapshot entirely.
	raw, err := ds.Sync(tctx, docID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "ghost-block") {
		t.Fatalf("GC'd tombstone still in snapshot: %s", raw)
	}
	if !strings.Contains(string(raw), "fresh") {
		t.Fatalf("live content missing from snapshot: %s", raw)
	}
}
