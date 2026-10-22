package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_tenant is the static-mode tenant registry. In row-level-security
// mode a tenant is just a string on each row, so nothing records that one
// exists until it writes. A hosted server needs that record to answer
// POST/GET /admin/tenants and to validate the tenantId on a new API key.
//
// Deliberately NOT under RLS: the registry is instance-global and the admin
// API reads it with no tenant on the context. This mirrors fabriq_api_key
// (0027) and fabriq_dynamic_entity (0033). Catalog mode keeps its own
// directory (fabriq_tenant_catalog) and never touches this table.
var migration0034Tenant = &migrate.Migration{
	Name:    "tenant",
	Version: "202610210034",
	Comment: "static-mode tenant registry (instance-global, no RLS)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_tenant (
				id         TEXT PRIMARY KEY,
				label      TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_tenant`,
		})
	},
}
