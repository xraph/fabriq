package agent

import (
	"testing"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestDelete_RemovesL0AndCollapsesCluster verifies that deleting a distillable
// source row (a) removes its L0 digest node and vector, (b) unlinks it from its
// parents, and (c) a subsequent Rollup re-rolls the remaining members and
// collapses the now below-noise-floor cluster node without error.
func TestDelete_RemovesL0AndCollapsesCluster(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d, w := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
	ctx := testCtx(t, "acme")

	for _, n := range []map[string]any{
		{"id": "n1", "title": "A", "body": "x", "site_id": "s1"},
		{"id": "n2", "title": "B", "body": "y", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	removed, err := d.DeleteL0(ctx, "note", "n1")
	if err != nil || !removed {
		t.Fatalf("delete n1: removed=%v err=%v", removed, err)
	}

	// The L0 node row must be gone.
	if _, ok, _ := d.getNode(ctx, L0ID("note", "n1")); ok {
		t.Fatal("deleted L0 node must be gone")
	}

	// The L0 vector must be gone.
	var matches []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: DigestEntity, Embedding: []float32{1, 0, 0}, K: 50}, &matches); err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		if m.ID == L0ID("note", "n1") {
			t.Fatal("deleted node's vector must be removed")
		}
	}

	// Re-roll over the remaining member must succeed (cluster collapses below floor).
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	// Idempotent delete: a second delete of the same id reports removed=false.
	if again, err := d.DeleteL0(ctx, "note", "n1"); err != nil || again {
		t.Fatalf("second delete must be a no-op: removed=%v err=%v", again, err)
	}
}

// TestRollup_CollapsesEmptyTenantRoot verifies that when all L0 nodes are
// deleted and Rollup runs, the vestigial tenant root is removed rather than
// re-summarized with empty children. A subsequent Rollup on an already-empty
// tree is also a no-error no-op (idempotent).
func TestRollup_CollapsesEmptyTenantRoot(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d, _ := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
	ctx := testCtx(t, "acme")

	// Build two L0 nodes and roll up so the tenant root exists.
	for _, n := range []map[string]any{
		{"id": "n1", "title": "Alpha", "body": "aaa", "site_id": "s1"},
		{"id": "n2", "title": "Beta", "body": "bbb", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	// Tenant root must exist after the first rollup.
	if _, ok, err := d.getNode(ctx, TenantRootID()); err != nil || !ok {
		t.Fatalf("tenant root should exist after rollup: ok=%v err=%v", ok, err)
	}

	// Delete both L0 nodes then rollup — root must be gone.
	for _, id := range []string{"n1", "n2"} {
		if removed, err := d.DeleteL0(ctx, "note", id); err != nil || !removed {
			t.Fatalf("delete %s: removed=%v err=%v", id, removed, err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatalf("rollup after full delete: %v", err)
	}
	if _, ok, _ := d.getNode(ctx, TenantRootID()); ok {
		t.Fatal("tenant root must be gone after all L0 nodes are deleted")
	}

	// Rollup again on empty tree — no error and root stays gone (idempotent).
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatalf("second rollup on empty tree: %v", err)
	}
	if _, ok, _ := d.getNode(ctx, TenantRootID()); ok {
		t.Fatal("tenant root must still be gone after second empty rollup")
	}
}
