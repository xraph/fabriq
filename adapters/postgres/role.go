package postgres

import (
	"context"
	"fmt"
)

// BypassesRLS reports whether the role this adapter connects as is exempt
// from row-level security: a superuser, or a role with BYPASSRLS. Tenant
// isolation in the shared-database modes rests on RLS alone, so a caller that
// serves tenants checks this before it accepts traffic.
//
// A role missing from pg_roles (which only happens if it was dropped under a
// live connection) reports an error rather than a guess.
func (a *Adapter) BypassesRLS(ctx context.Context) (bool, error) {
	var bypass bool
	err := a.pg.QueryRow(ctx,
		`SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user`,
	).Scan(&bypass)
	if err != nil {
		return false, fmt.Errorf("fabriq: read the serving role's attributes: %w", err)
	}
	return bypass, nil
}
