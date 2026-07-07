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

// AppendEvents dedupes on the natural key via the ORDER BY of the events RMT;
// re-appending the same (tenant, aggregate, agg_id, version) is a no-op. _dedup
// is 0 at ingest (only reprojection bumps it).
func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO fabriq_analytics_events (tenant_id, aggregate, agg_id, version, type, payload, at, _dedup)")
	if err != nil {
		return fmt.Errorf("fabriq: analytics append event: %w", err)
	}
	for _, e := range events {
		if err := batch.Append(e.TenantID, e.Aggregate, e.AggID, e.Version,
			e.Type, string(e.Payload), e.At, uint64(0)); err != nil {
			return fmt.Errorf("fabriq: analytics append event: %w", err)
		}
	}
	return batch.Send()
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

// LagByTenant reports now() - (that tenant's newest fact commit time), in
// seconds, per tenant. The winning row per aggregate has the newest at, so
// max(at) needs no FINAL. An empty table yields no rows -> empty map.
func (s *Sink) LagByTenant(ctx context.Context) (map[string]float64, error) {
	rows, err := s.conn.Query(ctx,
		`SELECT tenant_id, dateDiff('second', max(at), now64(3))
		 FROM fabriq_analytics_facts GROUP BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("fabriq: analytics lag: %w", err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var tid string
		var secs int64
		if err := rows.Scan(&tid, &secs); err != nil {
			return nil, fmt.Errorf("fabriq: analytics lag scan: %w", err)
		}
		out[tid] = float64(secs)
	}
	return out, rows.Err()
}

// ReprojectTenant re-writes stored fact and event payloads for a tenant (and
// optional aggregate) through transform, in place. ClickHouse's
// ReplacingMergeTree can't UPDATE a row, so instead: read the CURRENT
// winning row per key via argMax(col, _dedup) (facts: payload, version, at,
// deleted; events: payload, type, at) plus max(_dedup), transform the
// payload in Go, and re-INSERT only the rows whose bytes actually changed
// with _dedup = max(_dedup) + 1 — that stays within the domain version's
// 2^20 reprojection band, so the rewrite wins the next merge while
// argMax/max reads see it immediately. The count is computed by byte
// comparison in Go, so it is exact and idempotent regardless of merge state.
func (s *Sink) ReprojectTenant(ctx context.Context, tenantID, aggregate string,
	transform func(payload json.RawMessage) (json.RawMessage, error)) (int64, error) {
	var total int64

	// --- facts ---
	fr, err := s.conn.Query(ctx,
		`SELECT aggregate, agg_id,
		        argMax(payload, _dedup)  AS payload,
		        argMax(version, _dedup)  AS version,
		        argMax(at, _dedup)       AS at,
		        argMax(deleted, _dedup)  AS deleted,
		        max(_dedup)              AS dd
		 FROM fabriq_analytics_facts
		 WHERE tenant_id = ? AND (? = '' OR aggregate = ?)
		 GROUP BY aggregate, agg_id`, tenantID, aggregate, aggregate)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics reproject scan facts: %w", err)
	}
	type factRewrite struct {
		agg, aggID string
		version    int64
		at         time.Time
		deleted    uint8
		payload    string
		dd         uint64
	}
	var factRewrites []factRewrite
	for fr.Next() {
		var r factRewrite
		if err := fr.Scan(&r.agg, &r.aggID, &r.payload, &r.version, &r.at, &r.deleted, &r.dd); err != nil {
			fr.Close()
			return total, fmt.Errorf("fabriq: analytics reproject fact scan: %w", err)
		}
		np, err := transform(json.RawMessage(r.payload))
		if err != nil {
			fr.Close()
			return total, fmt.Errorf("fabriq: analytics reproject fact %s/%s: %w", r.agg, r.aggID, err)
		}
		if string(np) != r.payload {
			r.payload = string(np)
			factRewrites = append(factRewrites, r)
		}
	}
	if err := fr.Err(); err != nil {
		fr.Close()
		return total, fmt.Errorf("fabriq: analytics reproject facts: %w", err)
	}
	fr.Close()
	if len(factRewrites) > 0 {
		batch, err := s.conn.PrepareBatch(ctx,
			"INSERT INTO fabriq_analytics_facts (tenant_id, aggregate, agg_id, version, payload, at, deleted, _dedup)")
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject reinsert facts: %w", err)
		}
		for _, r := range factRewrites {
			if err := batch.Append(tenantID, r.agg, r.aggID, r.version, r.payload, r.at, r.deleted, r.dd+1); err != nil {
				return total, fmt.Errorf("fabriq: analytics reproject reinsert facts: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject reinsert facts: %w", err)
		}
		total += int64(len(factRewrites))
	}

	// --- events ---
	er, err := s.conn.Query(ctx,
		`SELECT aggregate, agg_id, version,
		        argMax(payload, _dedup) AS payload,
		        argMax(type, _dedup)    AS type,
		        argMax(at, _dedup)      AS at,
		        max(_dedup)             AS dd
		 FROM fabriq_analytics_events
		 WHERE tenant_id = ? AND (? = '' OR aggregate = ?)
		 GROUP BY aggregate, agg_id, version`, tenantID, aggregate, aggregate)
	if err != nil {
		return total, fmt.Errorf("fabriq: analytics reproject scan events: %w", err)
	}
	type eventRewrite struct {
		agg, aggID, typ string
		version         int64
		at              time.Time
		payload         string
		dd              uint64
	}
	var eventRewrites []eventRewrite
	for er.Next() {
		var r eventRewrite
		if err := er.Scan(&r.agg, &r.aggID, &r.version, &r.payload, &r.typ, &r.at, &r.dd); err != nil {
			er.Close()
			return total, fmt.Errorf("fabriq: analytics reproject event scan: %w", err)
		}
		np, err := transform(json.RawMessage(r.payload))
		if err != nil {
			er.Close()
			return total, fmt.Errorf("fabriq: analytics reproject event %s/%s/%d: %w", r.agg, r.aggID, r.version, err)
		}
		if string(np) != r.payload {
			r.payload = string(np)
			eventRewrites = append(eventRewrites, r)
		}
	}
	if err := er.Err(); err != nil {
		er.Close()
		return total, fmt.Errorf("fabriq: analytics reproject events: %w", err)
	}
	er.Close()
	if len(eventRewrites) > 0 {
		batch, err := s.conn.PrepareBatch(ctx,
			"INSERT INTO fabriq_analytics_events (tenant_id, aggregate, agg_id, version, type, payload, at, _dedup)")
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject reinsert events: %w", err)
		}
		for _, r := range eventRewrites {
			if err := batch.Append(tenantID, r.agg, r.aggID, r.version, r.typ, r.payload, r.at, r.dd+1); err != nil {
				return total, fmt.Errorf("fabriq: analytics reproject reinsert events: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject reinsert events: %w", err)
		}
		total += int64(len(eventRewrites))
	}
	return total, nil
}

// PruneEvents deletes history events with at < olderThan across all tenants
// and returns the number of logical events removed. Count-then-DELETE: the
// count is taken before the lightweight delete, so a re-run at the same cutoff
// returns 0.
func (s *Sink) PruneEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	var n uint64
	if err := s.conn.QueryRow(ctx,
		`SELECT uniqExact((tenant_id, aggregate, agg_id, version))
		 FROM fabriq_analytics_events WHERE at < ?`, olderThan).Scan(&n); err != nil {
		return 0, fmt.Errorf("fabriq: analytics prune count: %w", err)
	}
	if n == 0 {
		return 0, nil
	}
	if err := s.conn.Exec(ctx,
		`DELETE FROM fabriq_analytics_events WHERE at < ?`, olderThan); err != nil {
		return 0, fmt.Errorf("fabriq: analytics prune delete: %w", err)
	}
	return int64(n), nil
}

func (s *Sink) MaintainPartitions(ctx context.Context, retention time.Duration) (created, dropped int, err error) {
	return 0, 0, nil
}

// PurgeTenant hard-deletes all of one tenant's rows across the three tables and
// returns the total logical rows removed. ClickHouse has no cross-table
// transaction, so this deletes table-by-table; it is idempotent and safe to
// re-run if interrupted.
func (s *Sink) PurgeTenant(ctx context.Context, tenantID string) (int64, error) {
	specs := []struct{ table, key string }{
		{"fabriq_analytics_facts", "(tenant_id, aggregate, agg_id)"},
		{"fabriq_analytics_events", "(tenant_id, aggregate, agg_id, version)"},
		{"fabriq_analytics_applied", "(tenant_id, aggregate, agg_id)"},
	}
	var total int64
	for _, sp := range specs {
		var n uint64
		if err := s.conn.QueryRow(ctx,
			"SELECT uniqExact("+sp.key+") FROM "+sp.table+" WHERE tenant_id = ?", tenantID).Scan(&n); err != nil {
			return total, fmt.Errorf("fabriq: analytics purge count %s: %w", sp.table, err)
		}
		if n == 0 {
			continue
		}
		if err := s.conn.Exec(ctx,
			"DELETE FROM "+sp.table+" WHERE tenant_id = ?", tenantID); err != nil {
			return total, fmt.Errorf("fabriq: analytics purge delete %s: %w", sp.table, err)
		}
		total += int64(n)
	}
	return total, nil
}
