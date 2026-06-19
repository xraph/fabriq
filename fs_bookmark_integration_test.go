//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/tenant"
)

func TestFsBookmarkCRUDAndUniqueness(t *testing.T) {
	ctx := context.Background()
	f, _, _ := openFsTest(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	n1, _ := f.CreateFolder(tctx, "", "a")
	n2, _ := f.CreateFolder(tctx, "", "b")

	id1, err := f.AddBookmark(tctx, "u-1", n1.ID, 0)
	if err != nil {
		t.Fatalf("AddBookmark: %v", err)
	}
	if _, err := f.AddBookmark(tctx, "u-1", n2.ID, 1); err != nil {
		t.Fatalf("AddBookmark 2: %v", err)
	}

	marks, err := f.ListUserBookmarks(tctx, "u-1")
	if err != nil || len(marks) != 2 || marks[0].NodeID != n1.ID {
		t.Fatalf("ListUserBookmarks = %+v, err %v", marks, err)
	}

	// Duplicate (user, node) is rejected.
	if _, err := f.AddBookmark(tctx, "u-1", n1.ID, 5); err == nil {
		t.Fatal("duplicate bookmark should be rejected")
	}

	if err := f.RemoveBookmark(tctx, id1); err != nil {
		t.Fatalf("RemoveBookmark: %v", err)
	}
	if marks, _ := f.ListUserBookmarks(tctx, "u-1"); len(marks) != 1 {
		t.Fatalf("after remove = %d, want 1", len(marks))
	}
}
