// Package pganalytics is the reference analytics.Sink: a shared Postgres
// analytics database with a tenant_id column on every table. It is the ONE
// place fabriq deliberately co-locates data from many tenants; the database
// and its credential are separate from tenant DBs and the catalog control DB.
package pganalytics

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/analytics"
)

// Sink writes the analytics read model. It ensures its own schema at Open
// (three CREATE TABLE IF NOT EXISTS, mirroring the catalog control DB) rather
// than joining fabriq's tenant migration chain — this is a self-contained
// operator store.
type Sink struct {
	db          *pgdriver.PgDB
	partitioned bool // fabriq_analytics_events is range-partitioned by month on `at`
}

var _ analytics.Sink = (*Sink)(nil)

// Option configures the sink at Open.
type Option func(*Sink)

// WithEventPartitioning creates the event log as a monthly range-partitioned
// table (PARTITION BY RANGE (at)) so retention becomes an instant DROP
// PARTITION instead of a delete-scan. Takes effect only on a FRESH database —
// CREATE TABLE IF NOT EXISTS will not convert an existing non-partitioned
// events table; migrating an existing deployment is a manual step.
func WithEventPartitioning() Option { return func(s *Sink) { s.partitioned = true } }

// Open dials the analytics database and ensures the schema.
// ensureSchema proves reachability (it requires a live connection).
func Open(ctx context.Context, dsn string, opts ...Option) (*Sink, error) {
	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("fabriq: open analytics db: %w", err)
	}
	s := &Sink{db: db}
	for _, o := range opts {
		o(s)
	}
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
		s.eventsDDL(),
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
	if s.partitioned {
		// A DEFAULT partition catches any write outside the maintained monthly
		// window (e.g. a backfill appending an event with a historical `at`), so
		// writes never fail even if the maintainer is behind. The maintainer
		// creates the current/next month partitions and drops aged ones.
		if _, err := s.db.Exec(ctx,
			`CREATE TABLE IF NOT EXISTS fabriq_analytics_events_default
			 PARTITION OF fabriq_analytics_events DEFAULT`); err != nil {
			return fmt.Errorf("fabriq: ensure analytics events default partition: %w", err)
		}
		if _, _, err := s.MaintainPartitions(ctx, 0); err != nil {
			return err
		}
	}
	return nil
}

// eventsDDL returns the events-table DDL — range-partitioned by `at` when the
// sink is in partitioned mode (the partition key must be in the primary key, so
// `at` joins it; dedup still keys on version, which has one fixed `at`), plain
// otherwise.
func (s *Sink) eventsDDL() string {
	if s.partitioned {
		return `CREATE TABLE IF NOT EXISTS fabriq_analytics_events (
			tenant_id TEXT NOT NULL,
			aggregate TEXT NOT NULL,
			agg_id    TEXT NOT NULL,
			version   BIGINT NOT NULL,
			type      TEXT NOT NULL,
			payload   JSONB NOT NULL,
			at        TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (tenant_id, aggregate, agg_id, version, at)
		) PARTITION BY RANGE (at)`
	}
	return `CREATE TABLE IF NOT EXISTS fabriq_analytics_events (
		tenant_id TEXT NOT NULL,
		aggregate TEXT NOT NULL,
		agg_id    TEXT NOT NULL,
		version   BIGINT NOT NULL,
		type      TEXT NOT NULL,
		payload   JSONB NOT NULL,
		at        TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (tenant_id, aggregate, agg_id, version)
	)`
}

// monthStart truncates t to the first instant of its month (UTC).
func monthStart(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

// MaintainPartitions (partitioned mode only) ensures the current and next two
// months have partitions and, when retention > 0, drops every monthly partition
// whose entire range is older than now-retention — the instant-reclaim path
// that replaces a delete-scan. Returns (created, dropped). A no-op when the sink
// is not partitioned.
//
//nolint:gocritic // multi-value result; naming would collide with body error locals
func (s *Sink) MaintainPartitions(ctx context.Context, retention time.Duration) (int, int, error) {
	if !s.partitioned {
		return 0, 0, nil
	}
	now := time.Now().UTC()
	created := 0
	// Ensure current + next two months exist (stay ahead of the write clock).
	for i := 0; i < 3; i++ {
		start := monthStart(now).AddDate(0, i, 0)
		end := start.AddDate(0, 1, 0)
		name := fmt.Sprintf("fabriq_analytics_events_%04d%02d", start.Year(), int(start.Month()))
		// Skip if it already exists (IF NOT EXISTS is not allowed on PARTITION OF).
		if s.partitionExists(ctx, name) {
			continue
		}
		q := fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF fabriq_analytics_events FOR VALUES FROM ('%s') TO ('%s')`,
			name, start.Format("2006-01-02"), end.Format("2006-01-02"))
		if _, err := s.db.Exec(ctx, q); err != nil {
			// A concurrent maintainer or a stray default-partition row in this
			// range can make one create fail; that is not fatal — the default
			// partition still catches those writes. Continue.
			continue
		}
		created++
	}

	dropped := 0
	if retention > 0 {
		cutoff := now.Add(-retention)
		names, err := s.agedPartitions(ctx, cutoff)
		if err != nil {
			return created, 0, err
		}
		for _, name := range names {
			if _, err := s.db.Exec(ctx, `DROP TABLE IF EXISTS `+name); err != nil {
				return created, dropped, fmt.Errorf("fabriq: drop analytics partition %s: %w", name, err)
			}
			dropped++
		}
	}
	return created, dropped, nil
}

func (s *Sink) partitionExists(ctx context.Context, name string) bool {
	rows, err := s.db.Query(ctx, `SELECT 1 FROM pg_class WHERE relname = $1`, name)
	if err != nil {
		return false
	}
	defer rows.Close()
	return rows.Next()
}

// agedPartitions returns monthly event partitions whose upper bound is <= cutoff
// (the whole partition is older than retention). The default partition is never
// returned.
func (s *Sink) agedPartitions(ctx context.Context, cutoff time.Time) ([]string, error) {
	// A monthly partition named ..._YYYYMM covers [YYYY-MM-01, next month). Its
	// upper bound <= cutoff iff the FIRST of the following month <= cutoff, i.e.
	// the partition's month is strictly before cutoff's month.
	rows, err := s.db.Query(ctx,
		`SELECT relname FROM pg_class
		 WHERE relname ~ '^fabriq_analytics_events_[0-9]{6}$'`)
	if err != nil {
		return nil, fmt.Errorf("fabriq: list analytics partitions: %w", err)
	}
	defer rows.Close()
	var aged []string
	cutoffMonth := monthStart(cutoff)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// parse YYYYMM suffix
		suffix := name[len("fabriq_analytics_events_"):]
		var y, m int
		if _, err := fmt.Sscanf(suffix, "%04d%02d", &y, &m); err != nil {
			continue
		}
		partEnd := time.Date(y, time.Month(m), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
		if !partEnd.After(cutoffMonth) { // partEnd <= cutoffMonth
			aged = append(aged, name)
		}
	}
	return aged, rows.Err()
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

// AppendEvents dedupes on the natural key. The conflict target must match the
// events primary key, which includes `at` in partitioned mode (`at` is fixed
// per version, so dedup semantics are unchanged).
func (s *Sink) AppendEvents(ctx context.Context, events []analytics.Event) error {
	conflict := "(tenant_id, aggregate, agg_id, version)"
	if s.partitioned {
		conflict = "(tenant_id, aggregate, agg_id, version, at)"
	}
	q := `INSERT INTO fabriq_analytics_events
		(tenant_id, aggregate, agg_id, version, type, payload, at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT ` + conflict + ` DO NOTHING`
	for _, e := range events {
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

// AllWatermarks returns every applied watermark for a tenant in one read.
func (s *Sink) AllWatermarks(ctx context.Context, tenantID string) ([]analytics.Watermark, error) {
	type wmRow struct {
		Aggregate string `grove:"aggregate"`
		AggID     string `grove:"agg_id"`
		Version   int64  `grove:"version"`
	}
	var rows []wmRow
	if err := s.db.NewRaw(
		`SELECT aggregate, agg_id, version FROM fabriq_analytics_applied WHERE tenant_id = $1`, tenantID,
	).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("fabriq: analytics all watermarks: %w", err)
	}
	out := make([]analytics.Watermark, len(rows))
	for i, r := range rows {
		out[i] = analytics.Watermark{TenantID: tenantID, Aggregate: r.Aggregate, AggID: r.AggID, Version: r.Version}
	}
	return out, nil
}

// LagByTenant reports now() - (that tenant's newest fact commit time), in
// seconds, per tenant. An empty map means no facts. Same
// failed-iteration-is-an-error discipline as Watermark.
func (s *Sink) LagByTenant(ctx context.Context) (map[string]float64, error) {
	rows, err := s.db.Query(ctx,
		`SELECT tenant_id, EXTRACT(EPOCH FROM (now() - max(at)))::float8
		 FROM fabriq_analytics_facts GROUP BY tenant_id`)
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
// semantic: the UPDATE only fires when the re-projected JSONB actually differs
// (payload IS DISTINCT FROM), so RowsAffected counts real rewrites and a re-run
// is a no-op — the fake compares its canonical bytes and reaches the same count.
func (s *Sink) ReprojectTenant(ctx context.Context, tenantID, aggregate string, transform func(json.RawMessage) (json.RawMessage, error)) (int64, error) {
	var total int64

	type factRow struct {
		Aggregate string `grove:"aggregate"`
		AggID     string `grove:"agg_id"`
		Payload   string `grove:"payload"`
	}
	var facts []factRow
	if err := s.db.NewRaw(
		`SELECT aggregate, agg_id, payload::text AS payload FROM fabriq_analytics_facts
		 WHERE tenant_id = $1 AND ($2 = '' OR aggregate = $2)`, tenantID, aggregate,
	).Scan(ctx, &facts); err != nil {
		return 0, fmt.Errorf("fabriq: analytics reproject scan facts: %w", err)
	}
	for _, f := range facts {
		np, err := transform(json.RawMessage(f.Payload))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject fact %s/%s: %w", f.Aggregate, f.AggID, err)
		}
		res, err := s.db.Exec(ctx,
			`UPDATE fabriq_analytics_facts SET payload = $1::jsonb, updated_at = now()
			 WHERE tenant_id = $2 AND aggregate = $3 AND agg_id = $4 AND payload IS DISTINCT FROM $1::jsonb`,
			string(np), tenantID, f.Aggregate, f.AggID)
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject update fact: %w", err)
		}
		n, _ := res.RowsAffected()
		total += n
	}

	type eventRow struct {
		Aggregate string `grove:"aggregate"`
		AggID     string `grove:"agg_id"`
		Version   int64  `grove:"version"`
		Payload   string `grove:"payload"`
	}
	var events []eventRow
	if err := s.db.NewRaw(
		`SELECT aggregate, agg_id, version, payload::text AS payload FROM fabriq_analytics_events
		 WHERE tenant_id = $1 AND ($2 = '' OR aggregate = $2)`, tenantID, aggregate,
	).Scan(ctx, &events); err != nil {
		return total, fmt.Errorf("fabriq: analytics reproject scan events: %w", err)
	}
	for _, e := range events {
		np, err := transform(json.RawMessage(e.Payload))
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject event %s/%s/%d: %w", e.Aggregate, e.AggID, e.Version, err)
		}
		res, err := s.db.Exec(ctx,
			`UPDATE fabriq_analytics_events SET payload = $1::jsonb
			 WHERE tenant_id = $2 AND aggregate = $3 AND agg_id = $4 AND version = $5 AND payload IS DISTINCT FROM $1::jsonb`,
			string(np), tenantID, e.Aggregate, e.AggID, e.Version)
		if err != nil {
			return total, fmt.Errorf("fabriq: analytics reproject update event: %w", err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

// PruneEvents deletes history events with at < olderThan across all tenants
// (the (at) index carries the scan) and returns the count removed. Facts are
// untouched. Idempotent.
func (s *Sink) PruneEvents(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.Exec(ctx, `DELETE FROM fabriq_analytics_events WHERE at < $1`, olderThan)
	if err != nil {
		return 0, fmt.Errorf("fabriq: analytics prune events: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
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
