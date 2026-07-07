// Package pganalytics is the reference analytics.Sink: a shared Postgres
// analytics database with a tenant_id column on every table. It is the ONE
// place fabriq deliberately co-locates data from many tenants; the database
// and its credential are separate from tenant DBs and the catalog control DB.
package pganalytics

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/analytics"
)

// Sink writes the analytics read model. It ensures its own schema at Open
// (three CREATE TABLE IF NOT EXISTS, mirroring the catalog control DB) rather
// than joining fabriq's tenant migration chain — this is a self-contained
// operator store.
type Sink struct {
	db *pgdriver.PgDB
}

var _ analytics.Sink = (*Sink)(nil)

// Open dials the analytics database and ensures the schema.
// ensureSchema proves reachability (it requires a live connection).
func Open(ctx context.Context, dsn string) (*Sink, error) {
	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("fabriq: open analytics db: %w", err)
	}
	s := &Sink{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the analytics database connection.
func (s *Sink) Close() error { return s.db.Close() }

func (s *Sink) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_facts (
			tenant_id  TEXT NOT NULL,
			aggregate  TEXT NOT NULL,
			agg_id     TEXT NOT NULL,
			version    BIGINT NOT NULL,
			payload    JSONB NOT NULL,
			at         TIMESTAMPTZ NOT NULL,
			deleted    BOOLEAN NOT NULL DEFAULT false,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant_id, aggregate, agg_id)
		)`,
		`CREATE INDEX IF NOT EXISTS fabriq_analytics_facts_agg_idx
			ON fabriq_analytics_facts (aggregate)`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_events (
			tenant_id TEXT NOT NULL,
			aggregate TEXT NOT NULL,
			agg_id    TEXT NOT NULL,
			version   BIGINT NOT NULL,
			type      TEXT NOT NULL,
			payload   JSONB NOT NULL,
			at        TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (tenant_id, aggregate, agg_id, version)
		)`,
		`CREATE INDEX IF NOT EXISTS fabriq_analytics_events_at_idx
			ON fabriq_analytics_events (at)`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_applied (
			tenant_id  TEXT NOT NULL,
			aggregate  TEXT NOT NULL,
			agg_id     TEXT NOT NULL,
			version    BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant_id, aggregate, agg_id)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("fabriq: ensure analytics schema: %w", err)
		}
	}
	return nil
}

// UpsertFacts version-gates: a row is updated only when the incoming version
// is strictly greater than the stored one.
func (s *Sink) UpsertFacts(ctx context.Context, facts []analytics.Fact) error {
	for _, f := range facts {
		const q = `INSERT INTO fabriq_analytics_facts
			(tenant_id, aggregate, agg_id, version, payload, at, deleted, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7, now())
			ON CONFLICT (tenant_id, aggregate, agg_id) DO UPDATE
			SET version = EXCLUDED.version, payload = EXCLUDED.payload,
			    at = EXCLUDED.at, deleted = EXCLUDED.deleted, updated_at = now()
			WHERE EXCLUDED.version > fabriq_analytics_facts.version`
		if _, err := s.db.Exec(ctx, q, f.TenantID, f.Aggregate, f.AggID, f.Version, string(f.Payload), f.At, f.Deleted); err != nil {
			return fmt.Errorf("fabriq: analytics upsert fact: %w", err)
		}
	}
	return nil
}

// AppendEvents dedupes on the natural key.
func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	for _, e := range events {
		const q = `INSERT INTO fabriq_analytics_events
			(tenant_id, aggregate, agg_id, version, type, payload, at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (tenant_id, aggregate, agg_id, version) DO NOTHING`
		if _, err := s.db.Exec(ctx, q, e.TenantID, e.Aggregate, e.AggID, e.Version, e.Type, string(e.Payload), e.At); err != nil {
			return fmt.Errorf("fabriq: analytics append event: %w", err)
		}
	}
	return nil
}

// SetWatermark advances monotonically.
func (s *Sink) SetWatermark(ctx context.Context, ws []analytics.Watermark) error {
	for _, w := range ws {
		const q = `INSERT INTO fabriq_analytics_applied
			(tenant_id, aggregate, agg_id, version, updated_at)
			VALUES ($1,$2,$3,$4, now())
			ON CONFLICT (tenant_id, aggregate, agg_id) DO UPDATE
			SET version = EXCLUDED.version, updated_at = now()
			WHERE EXCLUDED.version > fabriq_analytics_applied.version`
		if _, err := s.db.Exec(ctx, q, w.TenantID, w.Aggregate, w.AggID, w.Version); err != nil {
			return fmt.Errorf("fabriq: analytics set watermark: %w", err)
		}
	}
	return nil
}

// Watermark reads the highest applied version (0 if unknown). Follows the
// catalog.go Get pattern: a failed row iteration must surface as an error,
// not silently read as "no rows" (see adapters/postgres/catalog.go Get).
func (s *Sink) Watermark(ctx context.Context, tenantID, aggregate, aggID string) (int64, error) {
	rows, err := s.db.Query(ctx,
		`SELECT version FROM fabriq_analytics_applied
		 WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, tenantID, aggregate, aggID)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics watermark: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("fabriq: analytics watermark: %w", err)
		}
		return 0, nil
	}
	var v int64
	if err := rows.Scan(&v); err != nil {
		return 0, fmt.Errorf("fabriq: analytics watermark scan: %w", err)
	}
	return v, rows.Err()
}

// LagSeconds reports now() - (newest fact's commit time), in seconds.
// hasData is false when no facts exist yet (max(at) is NULL). Same
// failed-iteration-is-an-error discipline as Watermark.
func (s *Sink) LagSeconds(ctx context.Context) (float64, bool, error) {
	rows, err := s.db.Query(ctx,
		`SELECT EXTRACT(EPOCH FROM (now() - max(at)))::float8 FROM fabriq_analytics_facts`)
	if err != nil {
		return 0, false, fmt.Errorf("fabriq: analytics lag: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("fabriq: analytics lag: %w", err)
		}
		return 0, false, nil
	}
	var secs sql.NullFloat64
	if err := rows.Scan(&secs); err != nil {
		return 0, false, fmt.Errorf("fabriq: analytics lag scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("fabriq: analytics lag: %w", err)
	}
	if !secs.Valid { // empty table -> max(at) is NULL
		return 0, false, nil
	}
	return secs.Float64, true, nil
}

// PurgeTenant hard-deletes all of one tenant's rows across the three analytics
// tables and returns the total removed. Runs in one transaction so a tenant is
// never left half-erased. Idempotent.
func (s *Sink) PurgeTenant(ctx context.Context, tenantID string) (int64, error) {
	tx, err := s.db.BeginTxQuery(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics purge begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var total int64
	for _, table := range []string{
		"fabriq_analytics_facts",
		"fabriq_analytics_events",
		"fabriq_analytics_applied",
	} {
		res, err := tx.NewRaw(`DELETE FROM `+table+` WHERE tenant_id = $1`, tenantID).Exec(ctx)
		if err != nil {
			return 0, fmt.Errorf("fabriq: analytics purge %s: %w", table, err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("fabriq: analytics purge commit: %w", err)
	}
	return total, nil
}

// TruncateForTest clears all analytics tables. Test-only.
func TruncateForTest(ctx context.Context, s *Sink) error {
	_, err := s.db.Exec(ctx, `TRUNCATE fabriq_analytics_facts, fabriq_analytics_events, fabriq_analytics_applied`)
	return err
}
