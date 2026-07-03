package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// EVICTED with 0003 (spec 2026-07-03 db-per-tenant, Phase 1): the
// tag_readings hypertable is demo-owned (series tables are application-
// named; the TSQuerier takes the series as a parameter). The guarded
// hypertable DDL now ships inside domain.DemoDDL(). Versioned no-op; see
// 0003.
var migration0005Timescale = &migrate.Migration{
	Name:    "timescale",
	Version: "202606120005",
	Comment: "no-op (demo hypertable evicted to domain.DemoDDL)",
	Up:      func(_ context.Context, _ migrate.Executor) error { return nil },
	Down:    func(_ context.Context, _ migrate.Executor) error { return nil },
}
