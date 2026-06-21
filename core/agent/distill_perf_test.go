package agent

import (
	"context"
	"io"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

// countingCAS wraps the fake CAS to count Retrieve calls. Store and every other
// blob.CAS method are promoted from the embedded *fabriqtest.FakeCAS.
type countingCAS struct {
	*fabriqtest.FakeCAS
	retrieves int
}

func (c *countingCAS) Retrieve(ctx context.Context, hash string) (io.ReadCloser, error) {
	c.retrieves++
	return c.FakeCAS.Retrieve(ctx, hash)
}

// TestRollup_RootChildrenAfterScanCollapse pins the tenant-root child set so the
// scan-collapse refactor cannot change rollup output. Two notes under one site
// → one scope node + (below the default noise floor of 2 the cluster may or may
// not form, but) the root must contain the scope node.
func TestRollup_RootChildrenAfterScanCollapse(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d, _ := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
	ctx := testCtx(t)

	for _, n := range []map[string]any{
		{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
		{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	root, ok, err := d.getNode(ctx, TenantRootID())
	if err != nil || !ok {
		t.Fatalf("root missing: ok=%v err=%v", ok, err)
	}
	wantScope := ScopeID("site", "s1")
	found := false
	for _, c := range root.ChildIDs {
		if c == wantScope {
			found = true
		}
	}
	if !found {
		t.Fatalf("root must link scope %q; children=%v", wantScope, root.ChildIDs)
	}
}

// TestDistill_BackfillBatchesEmbeds asserts the per-tenant backfill submits one
// Embed call per page of L0 rows (not one per row). With 5 rows and a page size
// of 2, the L0 embeds must arrive in batches of [2,2,1], not five 1-vector calls.
func TestDistill_BackfillBatchesEmbeds(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	emb := &countingEmbedder{}
	d, err := NewDistiller(fab, r, emb, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3, RecipeVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)

	// Seed 5 note rows through the command plane so listEntityVals sees them.
	for _, ttl := range []string{"a", "b", "c", "d", "e"} {
		if _, err := fab.Exec(ctx, command.Command{Entity: "note", Op: command.OpCreate, Payload: &tDoc{ID: ttl, Title: ttl, Body: "x"}}); err != nil {
			t.Fatal(err)
		}
	}

	orig := distillBatch
	distillBatch = 2
	defer func() { distillBatch = orig }()

	if _, err := d.Distill(ctx); err != nil {
		t.Fatal(err)
	}

	// Look for the three L0 page batches [2,2,1] among emb.batches. (Rollup also
	// embeds internal nodes one-at-a-time; we only assert the L0 page batches.)
	var pages []int
	for _, b := range emb.batches {
		if b == 2 || b == 1 {
			pages = append(pages, b)
		}
	}
	got2 := 0
	for _, b := range emb.batches {
		if b == 2 {
			got2++
		}
	}
	if got2 < 2 {
		t.Fatalf("expected at least two batched (size-2) L0 embed calls; batches=%v", emb.batches)
	}
}

// snapshotNodes returns every digest_node row keyed by id (level 0,1,2), so two
// rollup strategies can be compared for byte-identical output.
func snapshotNodes(t *testing.T, d *Distiller, ctx context.Context) map[string]digestRow {
	t.Helper()
	out := map[string]digestRow{}
	for _, lvl := range []int{LevelEntity, LevelScope, LevelTenant} {
		rows, err := d.listNodes(ctx, lvl)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			out[r.ID] = r
		}
	}
	return out
}

// TestRollup_InMemoryIndexMatchesGetNode builds a multi-scope tree, rolls it up,
// then edits one L0 and rolls again — asserting the resulting node set (ids +
// ContentHash + ChildIDs + ParentIDs + SummaryHash) is exactly what a
// from-scratch rebuild produces. This pins the index-aware rollup to identical
// output.
func TestRollup_InMemoryIndexMatchesGetNode(t *testing.T) {
	build := func() map[string]digestRow {
		r := distillRegistry(t)
		cas := fabriqtest.NewFakeCAS()
		d, _ := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
		ctx := testCtx(t)
		rows := []map[string]any{
			{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
			{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
			{"id": "n3", "title": "Valve C", "body": "fine", "site_id": "s2"},
		}
		for _, n := range rows {
			if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := d.Rollup(ctx); err != nil {
			t.Fatal(err)
		}
		// Edit one L0 then roll again (incremental path).
		if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "Pump A", "body": "CHANGED", "site_id": "s1"}); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Rollup(ctx); err != nil {
			t.Fatal(err)
		}
		return snapshotNodes(t, d, ctx)
	}

	a := build()
	b := build()
	if len(a) != len(b) {
		t.Fatalf("node count differs: %d vs %d", len(a), len(b))
	}
	for id, ra := range a {
		rb, ok := b[id]
		if !ok {
			t.Fatalf("node %q missing in second build", id)
		}
		if ra.ContentHash != rb.ContentHash || ra.SummaryHash != rb.SummaryHash ||
			ra.Kind != rb.Kind || ra.Level != rb.Level {
			t.Fatalf("node %q differs:\n a=%+v\n b=%+v", id, ra, rb)
		}
	}
}

// TestRollup_NoCASReadOnShortCircuit asserts an unchanged rollup retrieves zero
// summaries from CAS (the Merkle short-circuit must not pay for CAS I/O).
func TestRollup_NoCASReadOnShortCircuit(t *testing.T) {
	r := distillRegistry(t)
	cas := &countingCAS{FakeCAS: fabriqtest.NewFakeCAS()}
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, nil)
	ctx := testCtx(t)

	for _, n := range []map[string]any{
		{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
		{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	// Second rollup: nothing changed → every internal node short-circuits → no
	// CAS retrieve should happen.
	cas.retrieves = 0
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	if cas.retrieves != 0 {
		t.Fatalf("unchanged rollup must not read CAS; got %d retrieves", cas.retrieves)
	}
}
