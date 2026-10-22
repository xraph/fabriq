package postgres

import "context"

// RegisteredTenants returns the ids in the static-mode tenant registry
// (fabriq_tenant, created by migration 0034), ordered by id. The table is
// instance-global and has no RLS, so this runs on the pool path, never in a
// tenant transaction. Catalog mode keeps its tenants in the catalog instead.
func (a *Adapter) RegisteredTenants(ctx context.Context) ([]string, error) {
	rows, err := a.pg.Query(ctx, `SELECT id FROM fabriq_tenant ORDER BY id`)
	if err != nil {
		return nil, translatePg("list", "tenant", "", err)
	}
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, translatePg("list", "tenant", "", err)
		}
		out = append(out, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translatePg("list", "tenant", "", err)
	}
	return out, nil
}
