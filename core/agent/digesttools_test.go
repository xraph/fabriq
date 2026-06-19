package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

func TestDigest_SummaryAndChildren(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	// Build a minimal tree: one L0 note → rollup creates scope + tenant root.
	if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "A", "body": "hello", "site_id": "s1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	// Toolkit with CAS wired.
	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}

	view, err := tk.Digest(ctx, TenantRootID())
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	// The tenant root must have a non-empty Summary retrieved from CAS.
	if view.Summary == "" {
		t.Fatal("expected non-empty Summary from CAS")
	}
	// The tenant root must have ≥1 child with a non-empty ContentHash.
	if len(view.Children) == 0 {
		t.Fatal("expected ≥1 child")
	}
	for _, c := range view.Children {
		if c.ContentHash == "" {
			t.Fatalf("child %s has empty ContentHash", c.ID)
		}
	}
	// The Node line must match the root's id.
	if view.Node.ID != TenantRootID() {
		t.Fatalf("Node.ID = %q, want %q", view.Node.ID, TenantRootID())
	}

	// Nil-CAS case: Digest returns empty Summary but no error.
	tkNoCAS, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	view2, err := tkNoCAS.Digest(ctx, TenantRootID())
	if err != nil {
		t.Fatalf("Digest (nil CAS): %v", err)
	}
	if view2.Summary != "" {
		t.Fatalf("nil-CAS Digest must have empty Summary, got %q", view2.Summary)
	}
	if view2.Node.ID != TenantRootID() {
		t.Fatalf("nil-CAS Node.ID = %q", view2.Node.ID)
	}
}

// TestDigest_NodeNotFound verifies a clear error when the node id is absent.
func TestDigest_NodeNotFound(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")
	_, err = tk.Digest(ctx, "digest:2:tenant")
	if err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

func TestMap_OutlineAndUnchanged(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "A", "body": "x", "site_id": "s1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3, CAS: cas})
	if err != nil {
		t.Fatal(err)
	}

	lines, err := tk.Map(ctx, MapRequest{})
	if err != nil || len(lines) == 0 {
		t.Fatalf("map: %d lines err=%v", len(lines), err)
	}

	// Every line must have a non-empty ContentHash.
	for _, l := range lines {
		if l.ContentHash == "" {
			t.Fatalf("line %s has empty ContentHash", l.ID)
		}
	}

	// Build known-hashes from the first pass and assert all nodes are Unchanged.
	known := map[string]string{}
	for _, l := range lines {
		known[l.ID] = l.ContentHash
	}
	again, err := tk.Map(ctx, MapRequest{KnownHashes: known})
	if err != nil {
		t.Fatalf("second map: %v", err)
	}
	for _, l := range again {
		if !l.Unchanged {
			t.Fatalf("node %s should be Unchanged on re-map", l.ID)
		}
	}
}
