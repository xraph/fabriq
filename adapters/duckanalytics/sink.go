//go:build duckdb

// Package duckanalytics is an embedded-DuckDB analytics.Sink: a columnar,
// in-process store with the full mutable SQL surface (ON CONFLICT/UPDATE/DELETE),
// so it satisfies the Sink contract like pganalytics. Compiled only with
// -tags duckdb (CGO). Conformance runs in-process against duckdb://:memory:.
package duckanalytics

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/xraph/fabriq/core/analytics"
)

// Sink writes the analytics read model to an embedded DuckDB database. It
// ensures its own schema at Open (three CREATE TABLE IF NOT EXISTS, mirroring
// pganalytics), rather than joining fabriq's tenant migration chain.
type Sink struct{ db *sql.DB }

var _ analytics.Sink = (*Sink)(nil)

// dsnPath extracts the DuckDB file path (or ":memory:") from a duckdb:// DSN:
// "duckdb://:memory:" -> ":memory:"; "duckdb:///abs/path" -> "/abs/path";
// "duckdb://rel" -> "rel".
func dsnPath(dsn string) string {
	p := strings.TrimPrefix(dsn, "duckdb://")
	if p == ":memory:" || p == "" {
		return ":memory:"
	}
	return p // duckdb:///abs -> /abs ; duckdb://rel -> rel
}

// Open dials the embedded DuckDB database and ensures the schema.
// ensureSchema proves reachability (it requires a live connection).
func Open(ctx context.Context, dsn string) (*Sink, error) {
	db, err := sql.Open("duckdb", dsnPath(dsn))
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics open duckdb: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fabriq: analytics ping duckdb: %w", err)
	}
	s := &Sink{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the embedded DuckDB database connection.
func (s *Sink) Close() error { return s.db.Close() }

func (s *Sink) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_facts (
			tenant_id VARCHAR NOT NULL, aggregate VARCHAR NOT NULL, agg_id VARCHAR NOT NULL,
			version BIGINT NOT NULL, payload VARCHAR NOT NULL, "at" TIMESTAMP NOT NULL,
			deleted BOOLEAN NOT NULL DEFAULT false,
			PRIMARY KEY (tenant_id, aggregate, agg_id))`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_events (
			tenant_id VARCHAR NOT NULL, aggregate VARCHAR NOT NULL, agg_id VARCHAR NOT NULL,
			version BIGINT NOT NULL, type VARCHAR NOT NULL, payload VARCHAR NOT NULL, "at" TIMESTAMP NOT NULL,
			PRIMARY KEY (tenant_id, aggregate, agg_id, version))`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_applied (
			tenant_id VARCHAR NOT NULL, aggregate VARCHAR NOT NULL, agg_id VARCHAR NOT NULL,
			version BIGINT NOT NULL,
			PRIMARY KEY (tenant_id, aggregate, agg_id))`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("fabriq: analytics ensure duckdb schema: %w", err)
		}
	}
	return nil
}

// TruncateForTest clears all analytics tables. Test-only.
func TruncateForTest(ctx context.Context, s *Sink) error {
	for _, tbl := range []string{"fabriq_analytics_facts", "fabriq_analytics_events", "fabriq_analytics_applied"} {
		if _, err := s.db.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
			return err
		}
	}
	return nil
}

var errUnimplemented = errors.New("fabriq: analytics duckdb method unimplemented")

// STUBS below match core/analytics/analytics.go's Sink interface exactly; the
// var _ analytics.Sink assertion above is the compile-time check. Real bodies
// land in a follow-up task — this skeleton only proves Open/Close/schema.

func (s *Sink) UpsertFacts(ctx context.Context, facts []analytics.Fact) error {
	return errUnimplemented
}

func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	return errUnimplemented
}

func (s *Sink) Watermark(ctx context.Context, tenantID, aggregate, aggID string) (int64, error) {
	return 0, errUnimplemented
}

func (s *Sink) SetWatermark(ctx context.Context, ws []analytics.Watermark) error {
	return errUnimplemented
}

func (s *Sink) AllWatermarks(ctx context.Context, tenantID string) ([]analytics.Watermark, error) {
	return nil, errUnimplemented
}

func (s *Sink) LagByTenant(ctx context.Context) (map[string]float64, error) {
	return nil, errUnimplemented
}

func (s *Sink) ReprojectTenant(ctx context.Context, tenantID, aggregate string, transform func(payload json.RawMessage) (json.RawMessage, error)) (int64, error) {
	return 0, errUnimplemented
}

func (s *Sink) PruneEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	return 0, errUnimplemented
}

// MaintainPartitions is a no-op: DuckDB is not partitioned by this sink.
func (s *Sink) MaintainPartitions(ctx context.Context, retention time.Duration) (int, int, error) {
	return 0, 0, nil
}

func (s *Sink) PurgeTenant(ctx context.Context, tenantID string) (int64, error) {
	return 0, errUnimplemented
}
