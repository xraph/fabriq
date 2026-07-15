package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/tenant"
)

// TestRequireUnscopedRollupCtx proves the guard itself: unscoped/no-scope
// ctx passes, a scoped ctx is rejected.
func TestRequireUnscopedRollupCtx(t *testing.T) {
	unscoped := tenant.MustWithTenant(context.Background(), "acme")
	if err := requireUnscopedRollupCtx(unscoped); err != nil {
		t.Fatalf("unscoped ctx: want nil, got %v", err)
	}

	scoped := tenant.MustWithScope(unscoped, "proj1")
	if err := requireUnscopedRollupCtx(scoped); err == nil {
		t.Fatal("scoped ctx: want error, got nil")
	}
}

// TestMaintainRollup_RejectsScopedCtx proves the guard is actually wired
// into MaintainRollup (Task 6's fold-in of the Task-4 "Important" note): a
// scoped ctx must error BEFORE any database access, so this needs no live
// Postgres — a zero-value *Adapter suffices since the guard trips first.
func TestMaintainRollup_RejectsScopedCtx(t *testing.T) {
	m := revenueMetric()
	unscoped := tenant.MustWithTenant(context.Background(), "acme")
	scoped := tenant.MustWithScope(unscoped, "proj1")

	a := &Adapter{}
	if err := a.MaintainRollup(scoped, m, time.Now()); err == nil {
		t.Fatal("MaintainRollup under scoped ctx: want error, got nil")
	}
}

// TestRollupRange_RejectsScopedCtx mirrors the above for RollupRange
// directly (MaintainRollup's own call to RollupRange would also trip the
// guard, but this asserts the RollupRange entry point independently, since
// it's a separately exported/documented seam).
func TestRollupRange_RejectsScopedCtx(t *testing.T) {
	m := revenueMetric()
	unscoped := tenant.MustWithTenant(context.Background(), "acme")
	scoped := tenant.MustWithScope(unscoped, "proj1")

	a := &Adapter{}
	if err := a.RollupRange(scoped, m, time.Time{}, time.Now()); err == nil {
		t.Fatal("RollupRange under scoped ctx: want error, got nil")
	}
}
