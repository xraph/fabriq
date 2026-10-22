package postgres

import (
	"context"
	"errors"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// sqlStateUndefinedTable is PostgreSQL's undefined_table.
const sqlStateUndefinedTable = "42P01"

// RegisteredTenants returns the ids in the static-mode tenant registry
// (fabriq_tenant, created by migration 0034), ordered by id. The table is
// instance-global and has no RLS, so this runs on the pool path, never in a
// tenant transaction. Catalog mode keeps its tenants in the catalog instead.
//
// A database that has not run migration 0034 yet has no registry, so it has
// no registered tenants: the missing table returns an empty list, not an
// error. A rollout can then run this binary before `fabriq migrate up`, and
// callers such as Stores.AllTenants behave as they did before the registry.
func (a *Adapter) RegisteredTenants(ctx context.Context) ([]string, error) {
	rows, err := a.pg.Query(ctx, `SELECT id FROM fabriq_tenant ORDER BY id`)
	if err != nil {
		return registeredTenantsErr(err)
	}
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return registeredTenantsErr(err)
		}
		out = append(out, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		// The driver can report a missing table here rather than from Query.
		return registeredTenantsErr(err)
	}
	return out, nil
}

// registeredTenantsErr translates a RegisteredTenants failure. A missing
// fabriq_tenant table means no registered tenants, so it is no error.
func registeredTenantsErr(err error) ([]string, error) {
	terr := translatePg("list", "tenant", "", err)
	var fe *fabriqerr.Error
	if errors.As(terr, &fe) && fe.Meta.SQLState == sqlStateUndefinedTable {
		return nil, nil
	}
	return nil, terr
}
