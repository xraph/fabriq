package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// TestRollup_BuildsScopeAndTenant verifies the bottom-up rollup: two notes under
// site s1 produce a scope (L1) node and a tenant (L2) root with children, and a
// second rollup with no L0 change short-circuits the tenant (no re-summarize).
func TestRollup_BuildsScopeAndTenant(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, w := newDistiller(t, r, cas, sum, nil)
	ctx := testCtx(t)

	// Two notes under site s1.
	for _, n := range []map[string]any{
		{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
		{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := d.Rollup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ScopeNodes < 1 || !rep.TenantRolled {
		t.Fatalf("expected a scope node and a tenant roll: %+v", rep)
	}
	// The tenant root must exist and have children.
	root, ok, err := d.getNode(ctx, TenantRootID())
	if err != nil || !ok {
		t.Fatalf("tenant root missing: ok=%v err=%v", ok, err)
	}
	if len(root.ChildIDs) == 0 {
		t.Fatal("tenant root has no children")
	}
	_ = w

	// Idempotent: a second rollup with no L0 change short-circuits the tenant.
	callsBefore := sum.calls
	rep2, err := d.Rollup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sum.calls != callsBefore {
		t.Fatalf("idempotent rollup must not re-summarize: before=%d after=%d", callsBefore, sum.calls)
	}
	if rep2.TenantRolled {
		t.Fatal("unchanged tenant must short-circuit, not roll")
	}
	if rep2.ShortCircuits == 0 {
		t.Fatal("idempotent rollup must record short-circuits")
	}
}
