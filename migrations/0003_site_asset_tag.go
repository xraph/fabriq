package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// EVICTED (spec 2026-07-03 db-per-tenant, Phase 1): the example domain
// tables (sites, assets, tags, tag_readings) no longer ship in the default
// chain — fabriq must not create unprefixed demo tables inside a database
// it shares with a host application. Their DDL lives in domain.DemoDDL()
// and is applied by test harnesses and the demo binaries (the PagesDDL
// pattern). The migration is retained as a versioned no-op so deployed
// databases' migration records stay consistent; databases that already ran
// the original keep their demo tables (harmless; drop manually if unwanted).
var migration0003SiteAssetTag = &migrate.Migration{
	Name:    "site_asset_tag",
	Version: "202606120003",
	Comment: "no-op (demo tables evicted to domain.DemoDDL)",
	Up:      func(_ context.Context, _ migrate.Executor) error { return nil },
	Down:    func(_ context.Context, _ migrate.Executor) error { return nil },
}
