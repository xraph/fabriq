package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// CatalogStore is the Postgres tenant catalog (catalog.Catalog): the
// db-per-tenant control plane, living in a dedicated CONTROL database —
// never in a tenant database. It has no RLS and no tenant context; access
// to the control DSN is the trust boundary.
//
// The control schema is a single table, ensured idempotently at open.
// (Deviation from the spec's migrations/control chain, recorded there: a
// one-table schema does not warrant a versioned chain yet; EnsureSchema
// becomes chain-managed the day it grows.)
type CatalogStore struct {
	db *pgdriver.PgDB
}

var _ catalog.Catalog = (*CatalogStore)(nil)

// OpenCatalog dials the control database and ensures the catalog schema.
func OpenCatalog(ctx context.Context, dsn string) (*CatalogStore, error) {
	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("fabriq: open catalog control db: %w", err)
	}
	s := &CatalogStore{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the control connection pool.
func (s *CatalogStore) Close() error { return s.db.Close() }

// Elector builds a leader elector on the CATALOG control database — the
// coordination point for catalog-mode singletons (the drift reconciler),
// which have no primary shard to elect on. Pick one key per role and keep
// it stable across versions.
func (s *CatalogStore) Elector(key int64, opts ...ElectorOption) *Elector {
	return newElector(s.db, key, opts...)
}

func (s *CatalogStore) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fabriq_tenant_catalog (
			tenant_id  TEXT PRIMARY KEY,
			cluster_id TEXT NOT NULL,
			db_name    TEXT NOT NULL,
			state      TEXT NOT NULL,
			version    TEXT NOT NULL DEFAULT '',
			schema     TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		// Idempotent upgrade for control databases created before schema mode.
		`ALTER TABLE fabriq_tenant_catalog ADD COLUMN IF NOT EXISTS schema TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS fabriq_tenant_catalog_state_idx
			ON fabriq_tenant_catalog (state)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("fabriq: ensure catalog schema: %w", err)
		}
	}
	return nil
}

// Get implements catalog.Catalog.
func (s *CatalogStore) Get(ctx context.Context, tenantID string) (catalog.Entry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT tenant_id, cluster_id, db_name, state, version, schema, updated_at
		 FROM fabriq_tenant_catalog WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return catalog.Entry{}, translatePg("catalog get", "tenant_catalog", "", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// A failed iteration also reports "no rows" — surface it as the
		// transport failure it is. Answering NotFound during a catalog
		// outage would get negative-cached by the directory and route a
		// LIVE tenant off for a full TTL.
		if err := rows.Err(); err != nil {
			return catalog.Entry{}, translatePg("catalog get", "tenant_catalog", "", err)
		}
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeNotFound,
			"tenant is not in the catalog.", fabriqerr.WithEntity("tenant", tenantID))
	}
	var e catalog.Entry
	var state string
	if err := rows.Scan(&e.TenantID, &e.ClusterID, &e.Database, &state, &e.Version, &e.Schema, &e.UpdatedAt); err != nil {
		return catalog.Entry{}, translatePg("catalog scan", "tenant_catalog", "", err)
	}
	e.State = catalog.State(state)
	return e, rows.Err()
}

// Put implements catalog.Catalog with optimistic concurrency on
// updated_at (zero = create).
func (s *CatalogStore) Put(ctx context.Context, e catalog.Entry) (catalog.Entry, error) {
	if err := catalog.ValidateEntry(e); err != nil {
		return catalog.Entry{}, err
	}

	if e.UpdatedAt.IsZero() {
		rows, err := s.db.Query(ctx,
			`INSERT INTO fabriq_tenant_catalog (tenant_id, cluster_id, db_name, state, version, schema, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())
			 ON CONFLICT (tenant_id) DO NOTHING
			 RETURNING updated_at`,
			e.TenantID, e.ClusterID, e.Database, string(e.State), e.Version, e.Schema)
		if err != nil {
			return catalog.Entry{}, translatePg("catalog create", "tenant_catalog", "", err)
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil { // transport failure, not a conflict
				return catalog.Entry{}, translatePg("catalog create", "tenant_catalog", "", err)
			}
			return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeAlreadyExists,
				"tenant is already in the catalog.", fabriqerr.WithEntity("tenant", e.TenantID))
		}
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			return catalog.Entry{}, translatePg("catalog create scan", "tenant_catalog", "", err)
		}
		e.UpdatedAt = ts
		return e, rows.Err()
	}

	rows, err := s.db.Query(ctx,
		`UPDATE fabriq_tenant_catalog
		 SET cluster_id = $2, db_name = $3, state = $4, version = $5, schema = $6, updated_at = clock_timestamp()
		 WHERE tenant_id = $1 AND updated_at = $7
		 RETURNING updated_at`,
		e.TenantID, e.ClusterID, e.Database, string(e.State), e.Version, e.Schema, e.UpdatedAt)
	if err != nil {
		return catalog.Entry{}, translatePg("catalog update", "tenant_catalog", "", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil { // transport failure, not a lost CAS
			return catalog.Entry{}, translatePg("catalog update", "tenant_catalog", "", err)
		}
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeVersionConflict,
			"catalog entry was modified concurrently.", fabriqerr.WithEntity("tenant", e.TenantID))
	}
	var ts time.Time
	if err := rows.Scan(&ts); err != nil {
		return catalog.Entry{}, translatePg("catalog update scan", "tenant_catalog", "", err)
	}
	e.UpdatedAt = ts
	return e, rows.Err()
}

// List implements catalog.Catalog: stable tenant-id order, keyset cursor.
func (s *CatalogStore) List(ctx context.Context, cursor catalog.Cursor, limit int) ([]catalog.Entry, catalog.Cursor, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(ctx,
		`SELECT tenant_id, cluster_id, db_name, state, version, schema, updated_at
		 FROM fabriq_tenant_catalog
		 WHERE tenant_id > $1
		 ORDER BY tenant_id
		 LIMIT $2`, string(cursor), limit)
	if err != nil {
		return nil, "", translatePg("catalog list", "tenant_catalog", "", err)
	}
	defer rows.Close()
	out := make([]catalog.Entry, 0, limit)
	for rows.Next() {
		var e catalog.Entry
		var state string
		if err := rows.Scan(&e.TenantID, &e.ClusterID, &e.Database, &state, &e.Version, &e.Schema, &e.UpdatedAt); err != nil {
			return nil, "", translatePg("catalog list scan", "tenant_catalog", "", err)
		}
		e.State = catalog.State(state)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", translatePg("catalog list rows", "tenant_catalog", "", err)
	}
	next := catalog.Cursor("")
	if len(out) == limit {
		next = catalog.Cursor(out[len(out)-1].TenantID)
	}
	return out, next, nil
}
