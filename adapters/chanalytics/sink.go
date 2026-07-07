// Package chanalytics is a ClickHouse-backed analytics.Sink: a columnar,
// append-oriented store for cross-tenant fleet reporting. Three
// ReplacingMergeTree tables mirror the pganalytics schema; a packed _dedup
// column carries the version-gate and reprojection ordering. Every read is
// merge-independent (max/argMax/count-then-DELETE), so behavior matches the
// pganalytics reference without forcing background merges.
package chanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/xraph/fabriq/core/analytics"
)

// Sink writes the analytics read model into ClickHouse. It ensures its own
// schema at Open (three CREATE TABLE IF NOT EXISTS), like pganalytics.
type Sink struct {
	conn driver.Conn
}

var _ analytics.Sink = (*Sink)(nil)

// dedupShift reserves the low 20 bits of _dedup for the reprojection sequence;
// the domain version occupies the high bits. Safe in UInt64 for domain
// versions up to ~2^44 and ~2^20 reprojections.
const dedupShift = 20

func packDedup(version int64) uint64 { return uint64(version) << dedupShift }

// Open dials ClickHouse and ensures the schema. Ping proves reachability.
func Open(ctx context.Context, dsn string) (*Sink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics parse clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("fabriq: analytics ping clickhouse: %w", err)
	}
	s := &Sink{conn: conn}
	if err := s.ensureSchema(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the ClickHouse connection.
func (s *Sink) Close() error { return s.conn.Close() }

func (s *Sink) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_facts (
			tenant_id String, aggregate String, agg_id String,
			version Int64, payload String, at DateTime64(3, 'UTC'),
			deleted UInt8, _dedup UInt64
		) ENGINE = ReplacingMergeTree(_dedup)
		ORDER BY (tenant_id, aggregate, agg_id)`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_events (
			tenant_id String, aggregate String, agg_id String, version Int64,
			type String, payload String, at DateTime64(3, 'UTC'), _dedup UInt64
		) ENGINE = ReplacingMergeTree(_dedup)
		ORDER BY (tenant_id, aggregate, agg_id, version)`,
		`CREATE TABLE IF NOT EXISTS fabriq_analytics_applied (
			tenant_id String, aggregate String, agg_id String, version Int64
		) ENGINE = ReplacingMergeTree(version)
		ORDER BY (tenant_id, aggregate, agg_id)`,
	}
	for _, stmt := range stmts {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("fabriq: analytics ensure clickhouse schema: %w", err)
		}
	}
	return nil
}

// TruncateForTest clears all analytics tables. Test-only.
func TruncateForTest(ctx context.Context, s *Sink) error {
	for _, tbl := range []string{"fabriq_analytics_facts", "fabriq_analytics_events", "fabriq_analytics_applied"} {
		if err := s.conn.Exec(ctx, "TRUNCATE TABLE "+tbl); err != nil {
			return err
		}
	}
	return nil
}

var errUnimplemented = errors.New("fabriq: analytics clickhouse method unimplemented")

// UpsertFacts version-gates via _dedup: ReplacingMergeTree keeps the row with
// the max _dedup per (tenant, aggregate, agg_id), so a higher domain version
// always wins and a stale insert is shadowed. rewrite_seq is 0 at ingest.
func (s *Sink) UpsertFacts(ctx context.Context, facts []analytics.Fact) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO fabriq_analytics_facts (tenant_id, aggregate, agg_id, version, payload, at, deleted, _dedup)")
	if err != nil {
		return fmt.Errorf("fabriq: analytics upsert fact: %w", err)
	}
	for _, f := range facts {
		var deleted uint8
		if f.Deleted {
			deleted = 1
		}
		if err := batch.Append(f.TenantID, f.Aggregate, f.AggID, f.Version,
			string(f.Payload), f.At, deleted, packDedup(f.Version)); err != nil {
			return fmt.Errorf("fabriq: analytics upsert fact: %w", err)
		}
	}
	return batch.Send()
}

func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	return errUnimplemented
}

// Watermark reads the highest applied version (0 if unknown). max() over an
// empty set returns 0 in ClickHouse, so the unknown case needs no special path.
func (s *Sink) Watermark(ctx context.Context, tenantID, aggregate, aggID string) (int64, error) {
	var v int64
	err := s.conn.QueryRow(ctx,
		`SELECT max(version) FROM fabriq_analytics_applied
		 WHERE tenant_id = ? AND aggregate = ? AND agg_id = ?`,
		tenantID, aggregate, aggID).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics watermark: %w", err)
	}
	return v, nil
}

// SetWatermark inserts into the applied table (ReplacingMergeTree(version)); a
// max(version) read is monotonic, so a lower late insert is ignored.
func (s *Sink) SetWatermark(ctx context.Context, ws []analytics.Watermark) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO fabriq_analytics_applied (tenant_id, aggregate, agg_id, version)")
	if err != nil {
		return fmt.Errorf("fabriq: analytics set watermark: %w", err)
	}
	for _, w := range ws {
		if err := batch.Append(w.TenantID, w.Aggregate, w.AggID, w.Version); err != nil {
			return fmt.Errorf("fabriq: analytics set watermark: %w", err)
		}
	}
	return batch.Send()
}

// AllWatermarks returns every applied watermark for a tenant in one read.
func (s *Sink) AllWatermarks(ctx context.Context, tenantID string) ([]analytics.Watermark, error) {
	rows, err := s.conn.Query(ctx,
		`SELECT aggregate, agg_id, max(version) FROM fabriq_analytics_applied
		 WHERE tenant_id = ? GROUP BY aggregate, agg_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics all watermarks: %w", err)
	}
	defer rows.Close()
	var out []analytics.Watermark
	for rows.Next() {
		var agg, aggID string
		var v int64
		if err := rows.Scan(&agg, &aggID, &v); err != nil {
			return nil, fmt.Errorf("fabriq: analytics all watermarks scan: %w", err)
		}
		out = append(out, analytics.Watermark{TenantID: tenantID, Aggregate: agg, AggID: aggID, Version: v})
	}
	return out, rows.Err()
}

func (s *Sink) LagByTenant(ctx context.Context) (map[string]float64, error) {
	return nil, errUnimplemented
}

func (s *Sink) ReprojectTenant(ctx context.Context, tenantID, aggregate string, transform func(payload json.RawMessage) (json.RawMessage, error)) (rowsRewritten int64, err error) {
	return 0, errUnimplemented
}

func (s *Sink) PruneEvents(ctx context.Context, olderThan time.Time) (rowsDeleted int64, err error) {
	return 0, errUnimplemented
}

func (s *Sink) MaintainPartitions(ctx context.Context, retention time.Duration) (created, dropped int, err error) {
	return 0, 0, nil
}

func (s *Sink) PurgeTenant(ctx context.Context, tenantID string) (rowsDeleted int64, err error) {
	return 0, errUnimplemented
}
