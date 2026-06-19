package agent

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// TestDistill_E2E_DeleteAndContentHashStability covers two spec scenarios in a
// single hermetic run:
//
//	Scenario 4 — ContentHash stability: re-rolling an unchanged tree leaves
//	every node's ContentHash identical (Merkle re-grounding property).
//
//	Scenario 3 — delete + re-roll: deleting an L0 node then running Rollup
//	removes the node; getNode returns ok=false.
func TestDistill_E2E_DeleteAndContentHashStability(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d, _ := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
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

	// Scenario 4: snapshot every node's ContentHash, re-roll unchanged → identical.
	before := snapshotHashes(t, d, ctx)
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	after := snapshotHashes(t, d, ctx)
	for id, h := range before {
		if after[id] != h {
			t.Fatalf("unchanged subtree %s changed ContentHash %s -> %s", id, h, after[id])
		}
	}

	// Scenario 3: delete n2, re-roll → its L0 gone.
	if _, err := d.DeleteL0(ctx, "note", "n2"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.getNode(ctx, L0ID("note", "n2")); ok {
		t.Fatal("n2 L0 must be gone")
	}
}

// snapshotHashes collects id→ContentHash for all digest nodes at every level.
func snapshotHashes(t *testing.T, d *Distiller, ctx context.Context) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, lvl := range []int{LevelEntity, LevelScope, LevelTenant} {
		for _, n := range mustListNodes(t, d, ctx, lvl) {
			out[n.ID] = n.ContentHash
		}
	}
	return out
}

// mustListNodes wraps d.listNodes and fatals on error.
func mustListNodes(t *testing.T, d *Distiller, ctx context.Context, level int) []digestRow {
	t.Helper()
	rows, err := d.listNodes(ctx, level)
	if err != nil {
		t.Fatalf("listNodes(level=%d): %v", level, err)
	}
	return rows
}
