package fabriq

import (
	"context"
	"errors"
	"testing"

	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// TestLiveNodeUnderTrashedAncestorFallback is the descendantsAdjacencyWalk
// twin of TestFsLiveNodeUnderTrashedAncestor (integration): the fabriqtest
// fake reports ErrRawSQLUnsupported from Query, so every subtree read here
// goes through the portable walk, which must keep the CTE's semantics — no
// deleted_at filter during traversal (a live node under a trashed ancestor
// is still found), live filter applied only to the final result. The state
// is constructed directly by moving a live node under an already-trashed
// folder (MoveNode resolves the target via GetNode, any state), mirroring
// what a move racing TrashNode's enumeration produces.
func TestLiveNodeUnderTrashedAncestorFallback(t *testing.T) {
	f := newFakeFabriq(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatalf("WithTenant: %v", err)
	}

	crate, err := f.CreateFolder(ctx, "", "crate")
	if err != nil {
		t.Fatalf("CreateFolder(crate): %v", err)
	}
	inner, err := f.CreateFolder(ctx, crate.ID, "inner")
	if err != nil {
		t.Fatalf("CreateFolder(inner): %v", err)
	}
	survivor, err := f.CreateFolder(ctx, "", "survivor")
	if err != nil {
		t.Fatalf("CreateFolder(survivor): %v", err)
	}

	if terr := f.TrashNode(ctx, crate.ID); terr != nil {
		t.Fatalf("TrashNode(crate): %v", terr)
	}
	if _, merr := f.MoveNode(ctx, survivor.ID, inner.ID); merr != nil {
		t.Fatalf("MoveNode(survivor -> trashed inner): %v", merr)
	}
	sn, err := f.GetNode(ctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetNode(survivor): %v", err)
	}
	if sn.DeletedAt != nil {
		t.Fatal("survivor must stay live after moving under a trashed folder")
	}

	// Traversal crosses the trashed intermediate: live-only result surfaces
	// the survivor, include-trashed surfaces the intermediate too.
	live, err := f.Descendants(ctx, crate.ID)
	if err != nil {
		t.Fatalf("Descendants(crate): %v", err)
	}
	if len(live) != 1 || live[0].ID != survivor.ID {
		t.Fatalf("Descendants(trashed crate) = %+v, want exactly [survivor %s]", live, survivor.ID)
	}
	all, err := f.descendantNodes(ctx, crate.ID, true)
	if err != nil {
		t.Fatalf("descendantNodes(includeTrashed): %v", err)
	}
	wantAll := []string{inner.ID, survivor.ID} // path order: /inner, /inner/survivor
	if len(all) != len(wantAll) {
		t.Fatalf("descendantNodes(includeTrashed) = %+v, want %v", all, wantAll)
	}
	for i, w := range wantAll {
		if all[i].ID != w {
			t.Fatalf("all[%d].ID = %q, want %q", i, all[i].ID, w)
		}
	}

	// Path descent is live-only: the trashed segments block resolution.
	if _, perr := f.GetNodeByPath(ctx, "/crate/inner/survivor"); !errors.Is(perr, fabriqerr.ErrNotFound) {
		t.Fatalf("GetNodeByPath through trashed segment = %v, want ErrNotFound", perr)
	}

	// RestoreNode heals the whole subtree through the fallback walk.
	if rerr := f.RestoreNode(ctx, crate.ID); rerr != nil {
		t.Fatalf("RestoreNode(crate): %v", rerr)
	}
	live, err = f.Descendants(ctx, crate.ID)
	if err != nil {
		t.Fatalf("Descendants after restore: %v", err)
	}
	if len(live) != 2 || live[0].ID != inner.ID || live[1].ID != survivor.ID {
		t.Fatalf("descendants after restore = %+v, want [inner survivor]", live)
	}
	got, err := f.GetNodeByPath(ctx, "/crate/inner/survivor")
	if err != nil {
		t.Fatalf("GetNodeByPath after restore: %v", err)
	}
	if got.ID != survivor.ID {
		t.Fatalf("GetNodeByPath after restore = %s, want %s", got.ID, survivor.ID)
	}
}
