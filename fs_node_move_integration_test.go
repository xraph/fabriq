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

func TestFsRenameDerivedPaths(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, _ := f.CreateFolder(tctx, "", "a")
	b, _ := f.CreateFolder(tctx, a.ID, "b")
	_, _ = f.CreateFile(tctx, b.ID, "f.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})
	ax, _ := f.CreateFolder(tctx, "", "ax")

	if _, err := f.RenameNode(tctx, a.ID, "a2"); err != nil {
		t.Fatalf("RenameNode: %v", err)
	}

	if p, err := f.NodePath(tctx, b.ID); err != nil || p != "/a2/b" {
		t.Fatalf("b path = %q err=%v, want /a2/b", p, err)
	}
	if fn, err := f.GetNodeByPath(tctx, "/a2/b/f.txt"); err != nil || fn.Name != "f.txt" {
		t.Fatalf("file not found at /a2/b/f.txt: %v", err)
	}
	if p, _ := f.NodePath(tctx, ax.ID); p != "/ax" {
		t.Fatalf("prefix sibling moved: %q", p)
	}
}

// TestFsMoveIsSingleWrite proves the new write model: moving a subtree
// touches only the moved node — descendants keep their version and
// updated_at (no bulk rewrite, no per-row commands).
func TestFsMoveIsSingleWrite(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	src, _ := f.CreateFolder(tctx, "", "src")
	dst, _ := f.CreateFolder(tctx, "", "dst")
	child, _ := f.CreateFolder(tctx, src.ID, "child")
	leaf, _ := f.CreateFile(tctx, child.ID, "leaf.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})

	childBefore, _ := f.GetNode(tctx, child.ID)
	leafBefore, _ := f.GetNode(tctx, leaf.ID)

	if _, err := f.MoveNode(tctx, src.ID, dst.ID); err != nil {
		t.Fatalf("MoveNode: %v", err)
	}

	if p, _ := f.NodePath(tctx, leaf.ID); p != "/dst/src/child/leaf.txt" {
		t.Fatalf("leaf path = %q, want /dst/src/child/leaf.txt", p)
	}
	childAfter, _ := f.GetNode(tctx, child.ID)
	leafAfter, _ := f.GetNode(tctx, leaf.ID)
	if childAfter.Version != childBefore.Version || leafAfter.Version != leafBefore.Version {
		t.Fatalf("descendant versions changed on move: child %d→%d leaf %d→%d",
			childBefore.Version, childAfter.Version, leafBefore.Version, leafAfter.Version)
	}
	if !childAfter.UpdatedAt.Equal(childBefore.UpdatedAt) {
		t.Fatal("descendant updated_at changed on move")
	}

	// Cycle guard still holds without the path prefix check.
	if _, err := f.MoveNode(tctx, dst.ID, child.ID); err == nil {
		t.Fatal("moving a folder into its own subtree must fail")
	}
	if _, err := f.MoveNode(tctx, dst.ID, dst.ID); err == nil {
		t.Fatal("moving a folder into itself must fail")
	}
	// Name conflict at destination still rejected.
	src2, _ := f.CreateFolder(tctx, "", "src")
	if _, err := f.MoveNode(tctx, src2.ID, dst.ID); !errors.Is(err, fabriq.ErrNodeNameConflict) {
		t.Fatalf("sibling conflict err = %v, want ErrNodeNameConflict", err)
	}
}
