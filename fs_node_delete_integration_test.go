//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsTrashRestoreRecursive(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	dir, _ := f.CreateFolder(tctx, "", "dir")
	_, _ = f.CreateFile(tctx, dir.ID, "a.txt", bytes.NewReader([]byte("a")), fabriq.CreateFileOpts{})

	if err := f.TrashNode(tctx, dir.ID); err != nil {
		t.Fatalf("TrashNode: %v", err)
	}
	// Trashed subtree disappears from live listings.
	if kids, _ := f.ListChildren(tctx, "", 50, 0); len(kids) != 0 {
		t.Fatalf("trashed dir still listed: %+v", kids)
	}
	if _, err := f.GetNodeByPath(tctx, "/dir/a.txt"); err == nil {
		t.Fatal("trashed file still resolvable by path")
	}

	if err := f.RestoreNode(tctx, dir.ID); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	if kids, _ := f.ListChildren(tctx, "", 50, 0); len(kids) != 1 {
		t.Fatalf("restore did not bring dir back: %+v", kids)
	}
}

func TestFsPermanentDeleteCascadesBlobs(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	dir, _ := f.CreateFolder(tctx, "", "d")
	file, _ := f.CreateFile(tctx, dir.ID, "f.bin", bytes.NewReader([]byte("bytes")), fabriq.CreateFileOpts{})
	fn, _ := f.GetNode(tctx, file.ID)

	if err := f.PermanentDeleteNode(tctx, dir.ID); err != nil {
		t.Fatalf("PermanentDeleteNode: %v", err)
	}
	// Node rows gone.
	if _, err := f.GetNode(tctx, file.ID); err == nil {
		t.Fatal("file node still present after permanent delete")
	}
	// The referenced blob_object is gone too (so Phase-4 GC reclaims bytes).
	var bo struct{ ID string }
	if err := f.Relational().Get(tctx, "blob_object", fn.BlobID, &bo); err == nil {
		t.Fatal("blob_object still present after permanent delete of its only node")
	}
}

func TestFsLockBlocksWrites(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")
	n, _ := f.CreateFolder(tctx, "", "locked")
	if err := f.LockNode(tctx, n.ID, "user-1"); err != nil {
		t.Fatalf("LockNode: %v", err)
	}
	if _, err := f.RenameNode(tctx, n.ID, "x"); !errors.Is(err, fabriq.ErrNodeLocked) {
		t.Fatalf("rename of locked = %v, want ErrNodeLocked", err)
	}
	if err := f.UnlockNode(tctx, n.ID); err != nil {
		t.Fatalf("UnlockNode: %v", err)
	}
	if _, err := f.RenameNode(tctx, n.ID, "x"); err != nil {
		t.Fatalf("rename after unlock: %v", err)
	}
}

func TestFsReplaceFileBumpsVersion(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")
	dir, _ := f.CreateFolder(tctx, "", "d")
	file, _ := f.CreateFile(tctx, dir.ID, "f.txt", bytes.NewReader([]byte("v1")), fabriq.CreateFileOpts{ContentType: "text/plain"})
	before, _ := f.GetNode(tctx, file.ID)

	ref, err := f.ReplaceFile(tctx, file.ID, bytes.NewReader([]byte("v2-longer")), fabriq.CreateFileOpts{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("ReplaceFile: %v", err)
	}
	if ref.Version <= before.Version {
		t.Fatalf("version not bumped: %d <= %d", ref.Version, before.Version)
	}
	after, _ := f.GetNode(tctx, file.ID)
	if after.BlobID == before.BlobID || after.Size != int64(len("v2-longer")) {
		t.Fatalf("replace did not swap blob/facets: %+v", after)
	}
	rc, _, err := f.GetBlob(tctx, after.BlobID)
	if err != nil {
		t.Fatalf("GetBlob new: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "v2-longer" {
		t.Fatalf("new bytes = %q", got)
	}
}
