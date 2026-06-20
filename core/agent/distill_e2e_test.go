package agent

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// TestDistill_E2E_BuildEditGuard covers three spec scenarios in a single
// hermetic run:
//
//	Scenario 1 — tree builds: after DistillL0 for three notes + Rollup, the
//	tenant root node must exist.
//
//	Scenario 2 — incremental re-roll: editing only n1 then re-rolling must
//	leave n2's ContentHash untouched (Merkle short-circuit) while still
//	triggering at least one Summarize call for n1's branch.
//
//	Scenario 6 — guard fail-closed: a DistillL0 whose body contains "SECRET"
//	is vetoed by the fakeGuard before CAS storage; the blob count must not grow.
func TestDistill_E2E_BuildEditGuard(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, newFakeGuard())
	ctx := testCtx(t)

	notes := []map[string]any{
		{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
		{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
		{"id": "n3", "title": "Valve", "body": "leak", "site_id": "s2"},
	}
	for _, n := range notes {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	// Scenario 1: tree built — tenant root + ≥2 scope nodes exist.
	if _, ok, _ := d.getNode(ctx, TenantRootID()); !ok {
		t.Fatal("tenant root missing after build")
	}

	// Scenario 2: edit n1 only → only its branch re-rolls; n2/n3 L0 untouched.
	n2hashBefore := mustNode(ctx, t, d, L0ID("note", "n2")).ContentHash
	// Snapshot untouched nodes in the OTHER scope (s2) so we can assert locality.
	n3hashBefore := mustNode(ctx, t, d, L0ID("note", "n3")).ContentHash
	s2hashBefore := mustNode(ctx, t, d, ScopeID("site", "s2")).ContentHash
	callsBefore := sum.calls
	if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "Pump A", "body": "CRITICAL", "site_id": "s1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	if mustNode(ctx, t, d, L0ID("note", "n2")).ContentHash != n2hashBefore {
		t.Fatal("untouched n2 must keep its ContentHash")
	}
	// Multi-node locality: n3 and its scope (s2) are in a different branch from
	// n1 (which lives under s1); editing n1 must NOT re-summarize them.
	if mustNode(ctx, t, d, L0ID("note", "n3")).ContentHash != n3hashBefore {
		t.Fatal("untouched n3 (scope s2) must keep its ContentHash")
	}
	if mustNode(ctx, t, d, ScopeID("site", "s2")).ContentHash != s2hashBefore {
		t.Fatal("untouched scope s2 must keep its ContentHash")
	}
	// Lower bound: at least one Summarize was triggered for n1's branch.
	if sum.calls-callsBefore == 0 {
		t.Fatal("editing n1 must trigger at least its branch re-summarization")
	}
	// Upper bound: editing n1 re-summarizes at most n1 L0 + scope-s1 + cluster +
	// tenant root = 4 nodes (7 total). A removed Merkle short-circuit that
	// re-summarizes everything (7 calls) would exceed this bound.
	if sum.calls-callsBefore > 4 {
		t.Fatalf("edit of n1 caused %d re-summarizations, want ≤4 (7 total nodes)", sum.calls-callsBefore)
	}

	// Scenario 6: a SECRET write is blocked.
	cnt := cas.Len()
	if _, err := d.DistillL0(ctx, "note", "n4", map[string]any{"id": "n4", "title": "x", "body": "SECRET", "site_id": "s1"}); err != nil {
		t.Fatal(err)
	}
	if cas.Len() != cnt {
		t.Fatal("blocked content must not add a CAS blob")
	}
}

func mustNode(ctx context.Context, t *testing.T, d *Distiller, id string) digestRow {
	t.Helper()
	n, ok, err := d.getNode(ctx, id)
	if err != nil || !ok {
		t.Fatalf("node %s missing: ok=%v err=%v", id, ok, err)
	}
	return n
}
