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
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

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

// UpsertFacts version-gates: a row is updated only when the incoming version
// is strictly greater than the stored one. duckdb-go's ON CONFLICT does not
// support a WHERE predicate on the DO UPDATE clause, so the gate is expressed
// as a CASE in each assigned column instead — a stale-version upsert becomes a
// no-op UPDATE (every column reassigned to its current value).
func (s *Sink) UpsertFacts(ctx context.Context, facts []analytics.Fact) error {
	const q = `INSERT INTO fabriq_analytics_facts
		(tenant_id, aggregate, agg_id, version, payload, "at", deleted)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT (tenant_id, aggregate, agg_id) DO UPDATE SET
			version = CASE WHEN excluded.version > fabriq_analytics_facts.version THEN excluded.version ELSE fabriq_analytics_facts.version END,
			payload = CASE WHEN excluded.version > fabriq_analytics_facts.version THEN excluded.payload ELSE fabriq_analytics_facts.payload END,
			"at" = CASE WHEN excluded.version > fabriq_analytics_facts.version THEN excluded."at" ELSE fabriq_analytics_facts."at" END,
			deleted = CASE WHEN excluded.version > fabriq_analytics_facts.version THEN excluded.deleted ELSE fabriq_analytics_facts.deleted END`
	for _, f := range facts {
		if _, err := s.db.ExecContext(ctx, q, f.TenantID, f.Aggregate, f.AggID, f.Version, string(f.Payload), f.At, f.Deleted); err != nil {
			return fmt.Errorf("fabriq: analytics upsert fact: %w", err)
		}
	}
	return nil
}

// AppendEvents dedupes on the natural key.
func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	const q = `INSERT INTO fabriq_analytics_events
		(tenant_id, aggregate, agg_id, version, type, payload, "at")
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT (tenant_id, aggregate, agg_id, version) DO NOTHING`
	for _, e := range events {
		if _, err := s.db.ExecContext(ctx, q, e.TenantID, e.Aggregate, e.AggID, e.Version, e.Type, string(e.Payload), e.At); err != nil {
			return fmt.Errorf("fabriq: analytics append event: %w", err)
		}
	}
	return nil
}

// Watermark reads the highest applied version (0 if unknown).
func (s *Sink) Watermark(ctx context.Context, tenantID, aggregate, aggID string) (int64, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT version FROM fabriq_analytics_applied WHERE tenant_id=? AND aggregate=? AND agg_id=?`,
		tenantID, aggregate, aggID)
	var v int64
	if err := row.Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("fabriq: analytics watermark: %w", err)
	}
	return v, nil
}

// SetWatermark advances monotonically (same CASE version-gate as UpsertFacts).
func (s *Sink) SetWatermark(ctx context.Context, ws []analytics.Watermark) error {
	const q = `INSERT INTO fabriq_analytics_applied
		(tenant_id, aggregate, agg_id, version)
		VALUES (?,?,?,?)
		ON CONFLICT (tenant_id, aggregate, agg_id) DO UPDATE SET
			version = CASE WHEN excluded.version > fabriq_analytics_applied.version THEN excluded.version ELSE fabriq_analytics_applied.version END`
	for _, w := range ws {
		if _, err := s.db.ExecContext(ctx, q, w.TenantID, w.Aggregate, w.AggID, w.Version); err != nil {
			return fmt.Errorf("fabriq: analytics set watermark: %w", err)
		}
	}
	return nil
}

// AllWatermarks returns every applied watermark for a tenant in one read.
func (s *Sink) AllWatermarks(ctx context.Context, tenantID string) ([]analytics.Watermark, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT aggregate, agg_id, version FROM fabriq_analytics_applied WHERE tenant_id = ?`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics all watermarks: %w", err)
	}
	defer rows.Close()
	var out []analytics.Watermark
	for rows.Next() {
		var w analytics.Watermark
		w.TenantID = tenantID
		if err := rows.Scan(&w.Aggregate, &w.AggID, &w.Version); err != nil {
			return nil, fmt.Errorf("fabriq: analytics all watermarks scan: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fabriq: analytics all watermarks: %w", err)
	}
	return out, nil
}

// LagByTenant reports now() - (that tenant's newest fact commit time), in
// seconds, per tenant. An empty map means no facts.
func (s *Sink) LagByTenant(ctx context.Context) (map[string]float64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, epoch(now()) - epoch(max("at")) FROM fabriq_analytics_facts GROUP BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics lag: %w", err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var tid string
		var secs sql.NullFloat64
		if err := rows.Scan(&tid, &secs); err != nil {
			return nil, fmt.Errorf("fabriq: analytics lag scan: %w", err)
		}
		if secs.Valid {
			out[tid] = secs.Float64
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fabriq: analytics lag: %w", err)
	}
	return out, nil
}

// ReprojectTenant re-writes stored fact and event payloads for a tenant (and
// optional aggregate) through transform, in place. Change detection is
// semantic (payload IS DISTINCT FROM), so a re-run with the same transform is
// a no-op. Row counts come from RETURNING 1 (scanned), not RowsAffected —
// duckdb-go's RowsAffected is not relied upon here.
func (s *Sink) ReprojectTenant(ctx context.Context, tenantID, aggregate string, transform func(json.RawMessage) (json.RawMessage, error)) (int64, error) {
	var total int64

	type factRow struct {
		Aggregate string
		AggID     string
		Payload   string
	}
	factRows, err := s.db.QueryContext(ctx,
		`SELECT aggregate, agg_id, payload FROM fabriq_analytics_facts WHERE tenant_id = ? AND (? = '' OR aggregate = ?)`,
		tenantID, aggregate, aggregate)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics reproject scan facts: %w", err)
	}
	var facts []factRow
	for factRows.Next() {
		var f factRow
		if err := factRows.Scan(&f.Aggregate, &f.AggID, &f.Payload); err != nil {
			factRows.Close()
			return 0, fmt.Errorf("fabriq: analytics reproject scan facts: %w", err)
		}
		facts = append(facts, f)
	}
	if err := factRows.Err(); err != nil {
		factRows.Close()
		return 0, fmt.Errorf("fabriq: analytics reproject scan facts: %w", err)
	}
	factRows.Close()

	for _, f := range facts {
		np, err := transform(json.RawMessage(f.Payload))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject fact %s/%s: %w", f.Aggregate, f.AggID, err)
		}
		n, err := s.countReturned(ctx,
			`UPDATE fabriq_analytics_facts SET payload = ? WHERE tenant_id = ? AND aggregate = ? AND agg_id = ? AND payload IS DISTINCT FROM ? RETURNING 1`,
			string(np), tenantID, f.Aggregate, f.AggID, string(np))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject update fact: %w", err)
		}
		total += n
	}

	type eventRow struct {
		Aggregate string
		AggID     string
		Version   int64
		Payload   string
	}
	eventRows, err := s.db.QueryContext(ctx,
		`SELECT aggregate, agg_id, version, payload FROM fabriq_analytics_events WHERE tenant_id = ? AND (? = '' OR aggregate = ?)`,
		tenantID, aggregate, aggregate)
	if err != nil {
		return total, fmt.Errorf("fabriq: analytics reproject scan events: %w", err)
	}
	var events []eventRow
	for eventRows.Next() {
		var e eventRow
		if err := eventRows.Scan(&e.Aggregate, &e.AggID, &e.Version, &e.Payload); err != nil {
			eventRows.Close()
			return total, fmt.Errorf("fabriq: analytics reproject scan events: %w", err)
		}
		events = append(events, e)
	}
	if err := eventRows.Err(); err != nil {
		eventRows.Close()
		return total, fmt.Errorf("fabriq: analytics reproject scan events: %w", err)
	}
	eventRows.Close()

	for _, e := range events {
		np, err := transform(json.RawMessage(e.Payload))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject event %s/%s/%d: %w", e.Aggregate, e.AggID, e.Version, err)
		}
		n, err := s.countReturned(ctx,
			`UPDATE fabriq_analytics_events SET payload = ? WHERE tenant_id = ? AND aggregate = ? AND agg_id = ? AND version = ? AND payload IS DISTINCT FROM ? RETURNING 1`,
			string(np), tenantID, e.Aggregate, e.AggID, e.Version, string(np))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject update event: %w", err)
		}
		total += n
	}
	return total, nil
}

// countReturned runs a DML query with RETURNING and counts the rows returned.
// Used instead of sql.Result.RowsAffected, whose duckdb-go accuracy is not
// relied upon.
func (s *Sink) countReturned(ctx context.Context, q string, args ...any) (int64, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

// PruneEvents deletes history events with "at" < olderThan across all tenants
// and returns the count removed. Facts are untouched. Idempotent.
func (s *Sink) PruneEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	n, err := s.countReturned(ctx,
		`DELETE FROM fabriq_analytics_events WHERE "at" < ? RETURNING 1`, olderThan)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics prune events: %w", err)
	}
	return n, nil
}

// MaintainPartitions is a no-op: DuckDB is not partitioned by this sink.
func (s *Sink) MaintainPartitions(ctx context.Context, retention time.Duration) (int, int, error) {
	return 0, 0, nil
}

// PurgeTenant hard-deletes all of one tenant's rows across the three analytics
// tables and returns the total removed. Runs in one transaction so a tenant is
// never left half-erased. Idempotent.
func (s *Sink) PurgeTenant(ctx context.Context, tenantID string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
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
		rows, err := tx.QueryContext(ctx, `DELETE FROM `+table+` WHERE tenant_id = ? RETURNING 1`, tenantID)
		if err != nil {
			return 0, fmt.Errorf("fabriq: analytics purge %s: %w", table, err)
		}
		var n int64
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, fmt.Errorf("fabriq: analytics purge %s: %w", table, err)
		}
		rows.Close()
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("fabriq: analytics purge commit: %w", err)
	}
	return total, nil
}
