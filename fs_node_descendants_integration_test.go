//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

// TestFsDescendantsAdjacency pins the CTE semantics: path order, prefix
// siblings excluded structurally, live filter applied to the result set
// (not the recursion), root excluded.
func TestFsDescendantsAdjacency(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, _ := f.CreateFolder(tctx, "", "a")
	ax, _ := f.CreateFolder(tctx, "", "ax") // prefix sibling: must NOT appear
	b, _ := f.CreateFolder(tctx, a.ID, "b")
	c, _ := f.CreateFile(tctx, b.ID, "c.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})
	z, _ := f.CreateFile(tctx, a.ID, "z.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})
	_ = ax

	desc, err := f.Descendants(tctx, a.ID)
	if err != nil {
		t.Fatalf("Descendants: %v", err)
	}
	// Path order: /a/b, /a/b/c.txt, /a/z.txt
	wantIDs := []string{b.ID, c.ID, z.ID}
	if len(desc) != len(wantIDs) {
		t.Fatalf("got %d descendants, want %d", len(desc), len(wantIDs))
	}
	for i, w := range wantIDs {
		if desc[i].ID != w {
			t.Fatalf("desc[%d].ID = %q, want %q", i, desc[i].ID, w)
		}
	}

	// Trash the subtree: live Descendants goes empty, but the trashed rows
	// are still reachable for delete flows (exercised via RestoreNode).
	if err := f.TrashNode(tctx, a.ID); err != nil {
		t.Fatalf("TrashNode: %v", err)
	}
	desc, err = f.Descendants(tctx, a.ID)
	if err != nil {
		t.Fatalf("Descendants after trash: %v", err)
	}
	if len(desc) != 0 {
		t.Fatalf("live descendants after trash = %d, want 0", len(desc))
	}
	if err := f.RestoreNode(tctx, a.ID); err != nil {
		t.Fatalf("RestoreNode (needs include-trashed subtree): %v", err)
	}
	desc, _ = f.Descendants(tctx, a.ID)
	if len(desc) != 3 {
		t.Fatalf("descendants after restore = %d, want 3", len(desc))
	}
}
