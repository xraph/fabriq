package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// EVICTED with 0003 (spec 2026-07-03 db-per-tenant, Phase 1): this
// migration only applied RLS to the demo tables (sites, assets, tags) —
// every real fabriq table carries its RLS in its own migration. The demo
// RLS now ships inside domain.DemoDDL(). Versioned no-op; see 0003.
//
// tag_readings never had RLS: Timescale columnstore forbids it — readings
// tenancy is enforced structurally by the TSQuerier plus the adapter's
// raw-SQL guard (docs/decisions/0006-timescale-rls.md).
var migration0004RLSPolicies = &migrate.Migration{
	Name:    "rls_policies",
	Version: "202606120004",
	Comment: "no-op (demo-table RLS evicted to domain.DemoDDL)",
	Up:      func(_ context.Context, _ migrate.Executor) error { return nil },
	Down:    func(_ context.Context, _ migrate.Executor) error { return nil },
}
