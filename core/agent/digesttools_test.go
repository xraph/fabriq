package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

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
