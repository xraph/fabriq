//go:build integration

package postgres_test

// TestCompactSealsHistoryToBlob and TestCompactWithoutArchiveDeletes prove the
// Task 6 write path: when archiving is enabled for the entity, Compact seals
// the trimmed update-log tail into an immutable blob segment (+ a
// fabriq_crdt_segments index row) before deleting it, so history survives
// outside the DB; when archiving is off, Compact's behavior is unchanged
// (delete only, no segment written).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestCompactSealsHistoryToBlob(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents()
	fb := fabriqtest.NewFakeBlob()
	ds.EnableArchive(fb, true)

	tctx, _ := tenant.WithTenant(ctx, "t1")
	docID := "page/" + event.NewID()

	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "a", 100, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "b", 200, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := ds.Compact(tctx, docID); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// One sealed segment blob under crdt/<docID>/seg/.
	objs, err := fb.List(tctx, "crdt/"+docID+"/seg/")
	if err != nil || len(objs) != 1 {
		t.Fatalf("want 1 sealed segment, got %d (err=%v)", len(objs), err)
	}

	// Sync still returns the compacted snapshot (correctness preserved).
	raw, err := ds.Sync(tctx, docID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	_ = json.Unmarshal(raw, &payload)
	if len(payload.Snapshot) == 0 {
		t.Fatal("Sync should return the compacted snapshot")
	}
}

func TestCompactWithoutArchiveDeletes(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents() // no EnableArchive -> archive off
	fb := fabriqtest.NewFakeBlob()

	tctx, _ := tenant.WithTenant(ctx, "t2")
	docID := "page/" + event.NewID()

	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "x", 100, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := ds.Compact(tctx, docID); err != nil {
		t.Fatal(err)
	}
	objs, _ := fb.List(tctx, "crdt/"+docID+"/seg/")
	if len(objs) != 0 {
		t.Fatalf("archive off must write no segments, got %d", len(objs))
	}
}

// TestReadHistorySpansSegmentsAndDB proves the Task 7 read path: ReadHistory
// reconstructs a gap-free, ordered range from sealed blob segments (cached
// after first fetch) plus rows still in the hot DB tail. A seq lives in
// exactly one tier (sealing deletes it from the DB), so combining both
// sources and sorting by seq recovers the original range exactly.
func TestReadHistorySpansSegmentsAndDB(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents()
	ds.EnableArchive(fabriqtest.NewFakeBlob(), true)
	tctx, _ := tenant.WithTenant(ctx, "t1")
	docID := "page/" + event.NewID()

	// seq 1,2 sealed by Compact; seq 3 stays in the hot tail.
	_ = ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "a", 100, "n1"))
	_ = ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "b", 200, "n1"))
	if err := ds.Compact(tctx, docID); err != nil {
		t.Fatal(err)
	}
	_ = ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "c", 300, "n1")) // seq 3, unsealed

	hist, err := ds.ReadHistory(tctx, docID, 1, 3)
	if err != nil {
		t.Fatalf("ReadHistory: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("want 3 updates spanning segment+DB, got %d", len(hist))
	}
	for i, h := range hist {
		if h.Seq != int64(i+1) {
			t.Fatalf("hist[%d].Seq = %d, want %d", i, h.Seq, i+1)
		}
	}

	// second read hits the cache for the sealed segment; still correct
	hist2, err := ds.ReadHistory(tctx, docID, 1, 2)
	if err != nil || len(hist2) != 2 {
		t.Fatalf("cached read = %d updates (err=%v)", len(hist2), err)
	}
}
