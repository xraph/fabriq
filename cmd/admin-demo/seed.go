package main

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// seedCount is how many product rows each demo tenant gets. It is comfortably
// above the adminapi default list page size (50) so the SPA exercises cursor
// pagination on the very first browse.
const seedCount = 60

// productStatuses cycles a few statuses across the seeded rows so the SPA has
// something to group/filter on.
var productStatuses = []string{"active", "draft", "archived"}

// seedProducts idempotently ensures tenant tid has at least want product rows.
// It runs every startup: it first counts existing rows for the tenant and skips
// seeding entirely when the catalogue is already populated, otherwise it tops up
// to want rows via the command executor under a tenant-stamped context.
//
// Writes go through fabriq.Exec (command.OpCreate), the only sanctioned write
// path for aggregates; reads go through the relational querier. Both are
// tenant-scoped structurally — the tenant comes from the context, not the row.
func seedProducts(ctx context.Context, f *fabriq.Fabriq, tid string, want int) error {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return fmt.Errorf("admin-demo: seed tenant %q: %w", tid, err)
	}

	existing, err := countProducts(tctx, f)
	if err != nil {
		return fmt.Errorf("admin-demo: count products for %q: %w", tid, err)
	}
	if existing >= want {
		return nil // already seeded — safe to re-run on every startup
	}

	for i := existing; i < want; i++ {
		n := i + 1
		_, execErr := f.Exec(tctx, command.Command{
			Entity: productEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"name":   fmt.Sprintf("%s Product %03d", tid, n),
				"sku":    fmt.Sprintf("%s-SKU-%04d", tid, n),
				"price":  float64(n)*1.5 + 9.99,
				"status": productStatuses[i%len(productStatuses)],
			},
		})
		if execErr != nil {
			return fmt.Errorf("admin-demo: seed product %d for %q: %w", n, tid, execErr)
		}
	}
	return nil
}

// countProducts returns the number of product rows visible to the tenant in ctx.
// It pages through the rows with a generous cap; the demo seed sizes (~60) are
// well within a single page.
func countProducts(ctx context.Context, f *fabriq.Fabriq) (int, error) {
	var rows []map[string]any
	q := query.ListQuery{Limit: 1000}
	if err := f.Relational().List(ctx, productEntity, q, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}
