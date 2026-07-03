//go:build integration

package fabriq_test

import (
	"context"
	"errors"
	"testing"

	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// TestFsLiveNodeUnderTrashedAncestor pins the deliberate traversal semantics
// of descendantNodes: the recursive CTE does NOT filter deleted_at while
// descending, so a live node under a trashed ancestor is still found. The
// state is reachable in production — a concurrent move can land a node inside
// a subtree after TrashNode's enumeration but before its batch commit — and
// this test constructs it directly: MoveNode resolves the target parent via
// GetNode (any state, including trashed), so moving a live node under an
// already-trashed folder succeeds and yields the exact post-race state.
//
// Pinned semantics:
//  1. Descendants(trashedRoot) surfaces the live node (traversal is
//     unfiltered; only the result set is live-filtered),
//  2. GetNodeByPath does NOT resolve through the trashed segment (path
//     descent is live-only per segment),
//  3. RestoreNode heals the whole subtree, live node included.
func TestFsLiveNodeUnderTrashedAncestor(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	crate, err := f.CreateFolder(tctx, "", "crate")
	if err != nil {
		t.Fatalf("CreateFolder(crate): %v", err)
	}
	inner, err := f.CreateFolder(tctx, crate.ID, "inner")
	if err != nil {
		t.Fatalf("CreateFolder(inner): %v", err)
	}
	survivor, err := f.CreateFolder(tctx, "", "survivor")
	if err != nil {
		t.Fatalf("CreateFolder(survivor): %v", err)
	}

	if terr := f.TrashNode(tctx, crate.ID); terr != nil {
		t.Fatalf("TrashNode(crate): %v", terr)
	}
	if _, merr := f.MoveNode(tctx, survivor.ID, inner.ID); merr != nil {
		t.Fatalf("MoveNode(survivor -> trashed inner): %v", merr)
	}
	sn, err := f.GetNode(tctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetNode(survivor): %v", err)
	}
	if sn.DeletedAt != nil {
		t.Fatal("survivor must stay live after moving under a trashed folder")
	}
	if sn.ParentID != inner.ID {
		t.Fatalf("survivor.ParentID = %q, want %q", sn.ParentID, inner.ID)
	}

	// 1. The live node is reachable through the trashed subtree: the trashed
	// intermediate (inner) is filtered from the result but NOT from the
	// recursion.
	desc, err := f.Descendants(tctx, crate.ID)
	if err != nil {
		t.Fatalf("Descendants(crate): %v", err)
	}
	if len(desc) != 1 || desc[0].ID != survivor.ID {
		t.Fatalf("Descendants(trashed crate) = %+v, want exactly [survivor %s]", desc, survivor.ID)
	}
	desc, err = f.Descendants(tctx, inner.ID)
	if err != nil {
		t.Fatalf("Descendants(inner): %v", err)
	}
	if len(desc) != 1 || desc[0].ID != survivor.ID {
		t.Fatalf("Descendants(trashed inner) = %+v, want exactly [survivor %s]", desc, survivor.ID)
	}

	// 2. Path descent is live-only: the trashed segments block resolution
	// even though the leaf itself is live.
	if _, perr := f.GetNodeByPath(tctx, "/crate/inner/survivor"); !errors.Is(perr, fabriqerr.ErrNotFound) {
		t.Fatalf("GetNodeByPath through trashed segment = %v, want ErrNotFound", perr)
	}

	// 3. RestoreNode heals the subtree (its include-trashed enumeration must
	// also cross the trashed intermediate to reach every node).
	if rerr := f.RestoreNode(tctx, crate.ID); rerr != nil {
		t.Fatalf("RestoreNode(crate): %v", rerr)
	}
	desc, err = f.Descendants(tctx, crate.ID)
	if err != nil {
		t.Fatalf("Descendants after restore: %v", err)
	}
	wantIDs := []string{inner.ID, survivor.ID} // path order: /inner, /inner/survivor
	if len(desc) != len(wantIDs) {
		t.Fatalf("descendants after restore = %+v, want %v", desc, wantIDs)
	}
	for i, w := range wantIDs {
		if desc[i].ID != w {
			t.Fatalf("desc[%d].ID = %q, want %q", i, desc[i].ID, w)
		}
	}
	got, err := f.GetNodeByPath(tctx, "/crate/inner/survivor")
	if err != nil {
		t.Fatalf("GetNodeByPath after restore: %v", err)
	}
	if got.ID != survivor.ID {
		t.Fatalf("GetNodeByPath after restore = %s, want %s", got.ID, survivor.ID)
	}
}
