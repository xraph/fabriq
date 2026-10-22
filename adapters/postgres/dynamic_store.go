package postgres

import (
	"context"
	"fmt"
)

// DynamicSpecRecord is one persisted dynamic entity descriptor, as stored in
// fabriq_dynamic_entity.
type DynamicSpecRecord struct {
	// Name is the entity name the descriptor registers.
	Name string
	// Spec is the descriptor as JSON. The adapter treats it as opaque; the
	// caller owns the format.
	Spec []byte
	// Revision starts at 1 and increases each time the stored spec changes.
	Revision int64
}

// SaveDynamicSpec upserts the descriptor for name and returns its revision.
// The revision moves only when the spec differs from what is stored (a JSONB
// comparison, so key order and whitespace do not count). Saving an unchanged
// spec returns the current revision and leaves the row alone.
//
// Like execDDL it runs on the pool path, never in a tenant transaction: the
// table is instance-global and has no RLS.
func (a *Adapter) SaveDynamicSpec(ctx context.Context, name string, spec []byte) (revision int64, err error) {
	if name == "" {
		return 0, fmt.Errorf("fabriq: dynamic spec name must not be empty")
	}
	// The spec goes in as text and is cast to jsonb server side. A []byte
	// argument would be sent as bytea.
	err = a.pg.QueryRow(ctx,
		`INSERT INTO fabriq_dynamic_entity (name, spec) VALUES ($1, $2::jsonb)
		 ON CONFLICT (name) DO UPDATE
		   SET spec = EXCLUDED.spec,
		       revision = fabriq_dynamic_entity.revision + 1,
		       updated_at = now()
		   WHERE fabriq_dynamic_entity.spec IS DISTINCT FROM EXCLUDED.spec
		 RETURNING revision`, name, string(spec)).Scan(&revision)
	if err == nil {
		return revision, nil
	}
	if !isNoRows(err) {
		return 0, translatePg("save", "dynamic_entity", name, err)
	}
	// No row came back: the conflict fired but the spec was unchanged, so the
	// WHERE clause skipped the update. Report the revision already stored.
	if err := a.pg.QueryRow(ctx,
		`SELECT revision FROM fabriq_dynamic_entity WHERE name = $1`, name,
	).Scan(&revision); err != nil {
		return 0, translatePg("save", "dynamic_entity", name, err)
	}
	return revision, nil
}

// DeleteDynamicSpec removes the descriptor for name. Deleting a name that was
// never saved is not an error.
func (a *Adapter) DeleteDynamicSpec(ctx context.Context, name string) error {
	if _, err := a.pg.Exec(ctx,
		`DELETE FROM fabriq_dynamic_entity WHERE name = $1`, name); err != nil {
		return translatePg("delete", "dynamic_entity", name, err)
	}
	return nil
}

// LoadDynamicSpecs returns every persisted descriptor, ordered by name.
func (a *Adapter) LoadDynamicSpecs(ctx context.Context) ([]DynamicSpecRecord, error) {
	rows, err := a.pg.Query(ctx,
		`SELECT name, spec::text, revision FROM fabriq_dynamic_entity ORDER BY name`)
	if err != nil {
		return nil, translatePg("load", "dynamic_entity", "", err)
	}
	var out []DynamicSpecRecord
	for rows.Next() {
		var (
			r    DynamicSpecRecord
			spec string
		)
		if err := rows.Scan(&r.Name, &spec, &r.Revision); err != nil {
			_ = rows.Close()
			return nil, translatePg("load", "dynamic_entity", "", err)
		}
		r.Spec = []byte(spec)
		out = append(out, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, translatePg("load", "dynamic_entity", "", err)
	}
	return out, nil
}
