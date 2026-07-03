package fabriq

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

// newFakeFabriq builds a *Fabriq wired to the fabriqtest in-memory World, so
// this test exercises descendantsAdjacencyWalk (the fake reports
// ErrRawSQLUnsupported from Query, forcing the fallback) without Docker.
func newFakeFabriq(t *testing.T) *Fabriq {
	t.Helper()
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	w := fabriqtest.NewWorld(reg)
	f, err := New(reg, Ports{Store: w.Store, Relational: w.Rel})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// TestDescendantsAdjacencyWalkFallback exercises Descendants ordering and
// TrashNode's subtree soft-delete through the fake-backed adjacency-walk
// fallback (fabriqtest.FakeRelational.Query always returns
// ErrRawSQLUnsupported, so descendantNodes must fall back to
// descendantsAdjacencyWalk). Regression guard for the fs-node-adjacency
// branch, which regressed fake-backed subtree reads to a raw-SQL CTE.
func TestDescendantsAdjacencyWalkFallback(t *testing.T) {
	f := newFakeFabriq(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatalf("WithTenant: %v", err)
	}

	root, err := f.CreateFolder(ctx, "", "root")
	if err != nil {
		t.Fatalf("CreateFolder(root): %v", err)
	}
	b, err := f.CreateFolder(ctx, root.ID, "b")
	if err != nil {
		t.Fatalf("CreateFolder(b): %v", err)
	}
	a, err := f.CreateFolder(ctx, root.ID, "a")
	if err != nil {
		t.Fatalf("CreateFolder(a): %v", err)
	}
	c, err := f.CreateFolder(ctx, a.ID, "c")
	if err != nil {
		t.Fatalf("CreateFolder(a/c): %v", err)
	}

	descs, err := f.descendantNodes(ctx, root.ID, true)
	if err != nil {
		t.Fatalf("descendantNodes: %v", err)
	}
	if len(descs) != 3 {
		t.Fatalf("descendantNodes: got %d nodes, want 3: %+v", len(descs), descs)
	}
	// Sorted by relative path: /a, /a/c, /b.
	wantOrder := []string{a.ID, c.ID, b.ID}
	for i, n := range descs {
		if n.ID != wantOrder[i] {
			t.Fatalf("descendantNodes[%d] = %s, want %s (full: %+v)", i, n.ID, wantOrder[i], descs)
		}
	}

	// TrashNode on the root must soft-delete the whole subtree (root + a + b + c).
	if terr := f.TrashNode(ctx, root.ID); terr != nil {
		t.Fatalf("TrashNode: %v", terr)
	}
	for _, id := range []string{root.ID, a.ID, b.ID, c.ID} {
		n, gerr := f.GetNode(ctx, id)
		if gerr != nil {
			t.Fatalf("GetNode(%s) after trash: %v", id, gerr)
		}
		if n.DeletedAt == nil {
			t.Fatalf("node %s: expected DeletedAt set after TrashNode", id)
		}
	}

	// includeTrashed=false must now exclude the trashed descendants.
	live, err := f.descendantNodes(ctx, root.ID, false)
	if err != nil {
		t.Fatalf("descendantNodes (live only): %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("descendantNodes(includeTrashed=false) after TrashNode: got %d, want 0: %+v", len(live), live)
	}
}
