// Package pganalytics is the reference analytics.Sink: a shared Postgres
// analytics database with a tenant_id column on every table. It is the ONE
// place fabriq deliberately co-locates data from many tenants; the database
// and its credential are separate from tenant DBs and the catalog control DB.
package pganalytics

import (
	"context"
	"fmt"

	"github.com/xraph/grove/drivers/pgdriver"
)

// Sink writes the analytics read model. It ensures its own schema at Open
// (three CREATE TABLE IF NOT EXISTS, mirroring the catalog control DB) rather
// than joining fabriq's tenant migration chain — this is a self-contained
// operator store.
type Sink struct {
	db *pgdriver.PgDB
}

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
