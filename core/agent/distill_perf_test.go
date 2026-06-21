package agent

import (
	"context"
	"io"
	"testing"

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
