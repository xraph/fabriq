//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsRenameRewritesDescendantPaths(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, _ := f.CreateFolder(tctx, "", "a")
	b, _ := f.CreateFolder(tctx, a.ID, "b")
	_, _ = f.CreateFile(tctx, b.ID, "f.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})
	// Sibling whose name shares a prefix with "a" must NOT be rewritten.
	ax, _ := f.CreateFolder(tctx, "", "ax")

	if _, err := f.RenameNode(tctx, a.ID, "a2"); err != nil {
		t.Fatalf("RenameNode: %v", err)
	}

	// Descendant paths rewritten under the new prefix.
	bn, err := f.GetNode(tctx, b.ID)
	if err != nil || bn.Path != "/a2/b" {
		t.Fatalf("b path = %q err=%v, want /a2/b", bn.Path, err)
	}
	fn, _ := f.GetNodeByPath(tctx, "/a2/b/f.txt")
	if fn.Name != "f.txt" {
		t.Fatalf("file not found at /a2/b/f.txt")
	}
	// Prefix-sibling untouched.
	axn, _ := f.GetNode(tctx, ax.ID)
	if axn.Path != "/ax" {
		t.Fatalf("prefix sibling clobbered: %q", axn.Path)
	}
}

func TestFsMoveReparentsAndRewrites(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	src, _ := f.CreateFolder(tctx, "", "src")
	dst, _ := f.CreateFolder(tctx, "", "dst")
	mid, _ := f.CreateFolder(tctx, src.ID, "mid")
	_, _ = f.CreateFile(tctx, mid.ID, "leaf.txt", bytes.NewReader([]byte("y")), fabriq.CreateFileOpts{})

	if _, err := f.MoveNode(tctx, mid.ID, dst.ID); err != nil {
		t.Fatalf("MoveNode: %v", err)
	}
	moved, _ := f.GetNode(tctx, mid.ID)
	if moved.ParentID != dst.ID || moved.Path != "/dst/mid" {
		t.Fatalf("moved = %+v", moved)
	}
	leaf, _ := f.GetNodeByPath(tctx, "/dst/mid/leaf.txt")
	if leaf.Name != "leaf.txt" {
		t.Fatalf("leaf not at new path")
	}

	// Cycle: cannot move a node into its own subtree.
	if _, err := f.MoveNode(tctx, dst.ID, mid.ID); err == nil {
		t.Fatal("moving dst into its descendant mid should fail")
	}
	// Name collision under destination.
	_, _ = f.CreateFolder(tctx, dst.ID, "src")
	if _, err := f.MoveNode(tctx, src.ID, dst.ID); !errors.Is(err, fabriq.ErrNodeNameConflict) {
		t.Fatalf("move collision = %v, want ErrNodeNameConflict", err)
	}
}
