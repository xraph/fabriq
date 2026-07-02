//go:build integration

package postgres_test

// TestOffloadSyncParity is the drift guard for the whole history-offload
// feature: it proves the observable CRDT contract is identical whether
// Compact seals trimmed history to blob segments (archive on) or simply
// deletes it (archive off). Both paths fold updates through the same
// mergedState, so the compacted Snapshot().Snapshot bytes must be
// byte-identical regardless of archiving — only the storage shape of the
// trimmed log differs.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestOffloadSyncParity(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)

	run := func(archive bool, docID, tid string) json.RawMessage {
		ds := app.Documents()
		if archive {
			ds.EnableArchive(fabriqtest.NewFakeBlob(), true)
		}
		tctx, _ := tenant.WithTenant(ctx, tid)
		for i, v := range []string{"a", "b", "c", "d"} {
			if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", v, int64(100*(i+1)), "n1")); err != nil {
				t.Fatal(err)
			}
		}
		if err := ds.Compact(tctx, docID); err != nil {
			t.Fatal(err)
		}
		snap, err := ds.Snapshot(tctx, docID)
		if err != nil {
			t.Fatal(err)
		}
		return snap.Snapshot
	}

	on := run(true, "page/"+event.NewID(), "pon")
	off := run(false, "page/"+event.NewID(), "poff")
	if string(on) != string(off) {
		t.Fatalf("snapshot parity broken:\n archive-on : %s\n archive-off: %s", on, off)
	}
}
