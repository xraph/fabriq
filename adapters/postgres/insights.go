package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// Track implements query.AnalyticsQuerier — the outbox-bypass customer-event
// ingest. One multi-row INSERT per call; dedup_key collisions are ignored
// (the unique partial index on (tenant_id, dedup_key) WHERE dedup_key IS NOT
// NULL enforces idempotency — NULL dedup keys never conflict).
func (a *Adapter) Track(ctx context.Context, events []query.AnalyticsEvent) error {
	if len(events) == 0 {
		return nil
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var sb strings.Builder
		// $1 = tenant_id (shared across all rows), $2 = scope arg (NULLIF converts
		// "" to NULL so unscoped writes store NULL, meaning "shared / no scope").
		args := make([]any, 0, len(events)*4+2)
		args = append(args, tid, tenant.ScopeOrEmpty(ctx))
		sb.WriteString(`INSERT INTO fabriq_insights_events (tenant_id, scope_id, name, at, props, dedup_key) VALUES `)
		for i, e := range events {
			if e.Props == nil {
				// Store {} rather than JSON null: the cube builder always reads
				// props via `props ->> 'key'`, which is well-defined (NULL) on an
				// empty object but would be an error to assume on a JSON null.
				e.Props = map[string]any{}
			}
			propsJSON, merr := json.Marshal(e.Props)
			if merr != nil {
				return fmt.Errorf("fabriq: insights track marshal props: %w", merr)
			}
			if i > 0 {
				sb.WriteByte(',')
			}
			n := len(args)
			// name=$n+1, at=$n+2, props=$n+3, dedup=$n+4
			fmt.Fprintf(&sb, "($1, NULLIF($2, ''), $%d, $%d, $%d::jsonb, NULLIF($%d, ''))", n+1, n+2, n+3, n+4)
			args = append(args, e.Name, e.At, string(propsJSON), e.DedupKey)
		}
		sb.WriteString(` ON CONFLICT (tenant_id, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`)
		if _, err := tx.NewRaw(sb.String(), args...).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: insights track %d events: %w", len(events), err)
		}
		return nil
	})
}

// InsightsAdapter wraps Adapter to implement query.AnalyticsQuerier.
//
// A separate type is required because *Adapter already carries Query for
// query.RelationalQuerier (ctx, into any, sql string, args ...any) — the
// raw-SQL escape hatch behind f.Relational().Query(...) (adapter.go:649).
// query.AnalyticsQuerier's Query has a different shape (ctx, AnalyticsQuery,
// into any); Go does not allow two methods named Query with different
// signatures on one type, so the cube-query variant lives here. This is the
// same collision VectorAdapter/SpatialAdapter (vector.go, spatial.go) were
// already introduced to resolve for Get/Upsert — InsightsAdapter follows
// their exact pattern: Track has no name collision and stays delegated to
// the existing *Adapter method for backward compat; Query, which collides,
// is implemented directly here instead of on *Adapter.
//
// QueryRaw (the read-only SQL escape hatch) rounds out the interface below.
type InsightsAdapter struct {
	a *Adapter
}

var _ query.AnalyticsQuerier = (*InsightsAdapter)(nil)

// NewInsightsAdapter wraps an existing Postgres adapter for the customer-facing
// analytics port.
func NewInsightsAdapter(a *Adapter) *InsightsAdapter { return &InsightsAdapter{a: a} }

// Track implements query.AnalyticsQuerier by delegating to *Adapter.Track
// (no name collision there).
func (i *InsightsAdapter) Track(ctx context.Context, events []query.AnalyticsEvent) error {
	return i.a.Track(ctx, events)
}

// Query implements query.AnalyticsQuerier — on-demand cube aggregation over
// the tenant's fabriq_insights_events table.
//
// This composes the pure buildInsightsSQL builder with the SAME dynamic-tx +
// scanMaps/assignMapsDest path List uses for map-native reads (adapter.go),
// rather than inTenantTx (used by Track above): *pgdriver.PgTx only exposes
// grove's query builder (NewSelect/NewInsert/NewUpdate/NewDelete/NewRaw),
// whose Scan targets a fixed struct/model type — there is no method that
// hands back a driver.Rows cursor for scanning into an arbitrary into.
// inDynamicTenantTx hands the closure a raw driver.Tx, whose Query() returns
// driver.Rows; scanMaps turns that into []map[string]any and
// assignMapsDest projects it into into, exactly as List's dynamic-entity
// path does.
func (i *InsightsAdapter) Query(ctx context.Context, q query.AnalyticsQuery, into any) error {
	// Tenant-first, mirroring fabriqtest.FakeAnalytics.Query (Task 6 aligns
	// the two): a no-tenant call must fail with the tenant error, not with
	// whatever ResolveSource happens to return for an unresolved Source. This
	// check is intentionally repeated inside inDynamicTenantTx below (which
	// derives tid itself) — cheap, and keeps this ordering guarantee local to
	// Query rather than depending on inDynamicTenantTx's internals.
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	d, err := insights.ResolveSource(i.a.reg, q.Source)
	if err != nil {
		return err
	}

	// Stitching-rollup routing (phase 2b, Task 5): a query naming a
	// materialized metric (MetricSpec.Rollup != nil) whose shape is
	// rollup-compatible is served by combining the sealed rollup with a
	// live tail (buildStitchedRollupSQL, insights_rollup_query.go) instead
	// of the fully-live path below. Descriptor does not carry RollupSpec —
	// it is shared with fabriqtest.FakeAnalytics, which never materializes
	// anything — so the metric is looked up again here, by name, directly
	// from the registry. EffectiveQuery is called early (rather than left
	// to buildInsightsSQL) so rollupCompatible can be checked against the
	// EFFECTIVE dimensions/bucket a metric-sourced query actually runs
	// with, not q's raw (and, for a metric source, always-empty)
	// Dimensions/TimeBucket fields.
	if d.FromMetric && i.a.reg != nil {
		if m, ok := i.a.reg.Metric(d.MetricName); ok && m.Rollup != nil {
			effMeasures, effDims, effBucket, eerr := insights.EffectiveQuery(q, d)
			if eerr != nil {
				return eerr
			}
			qEff := q
			qEff.Dimensions = effDims
			qEff.TimeBucket = effBucket
			// Route to the stitched builder ONLY when a watermark already
			// exists (review fix, phase 2b Task 5): buildStitchedRollupSQL's
			// sealed CTE unconditionally does `FROM fabriq_insights_rollup_
			// <metric>` — even when hasWatermark is false and the WHERE
			// clause degrades to "AND FALSE", Postgres still needs that
			// relation to exist at plan time. Since EnsureRollupTable isn't
			// wired into boot until a later task, a materialized metric that
			// has never had a maintainer pass (no watermark row, and
			// possibly no rollup table at all) would make Query() fail with
			// "relation does not exist" the moment MetricSpec.Rollup is set
			// — a regression, not the intended "still exactly correct, just
			// unaccelerated" behavior the sealed CTE's AND-FALSE branch is
			// meant to provide. Reading the watermark FIRST and gating the
			// branch on it means an un-rolled-up metric falls through to the
			// ordinary live path below (functionally identical to what the
			// sealed CTE's permanently-false branch would have computed, had
			// the table existed) with no dependency on the rollup table.
			//
			// ReadRollupWatermark reads fabriq_insights_rollup_state, which
			// always exists (migration 0032) independent of any per-metric
			// rollup table; a metric with no maintainer pass simply has no
			// state row, so hasWatermark is cleanly false here — never an
			// error caused by a missing per-metric table.
			if rollupCompatible(qEff, m) {
				watermark, hasWatermark, werr := i.a.ReadRollupWatermark(ctx, m.Name)
				if werr != nil {
					return werr
				}
				if hasWatermark {
					return i.a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
						sql, args, berr := i.a.buildStitchedRollupSQL(qEff, tid, m, effMeasures, effDims, effBucket, watermark, hasWatermark)
						if berr != nil {
							return berr
						}
						rows, qerr := tx.Query(ctx, sql, args...)
						if qerr != nil {
							return fmt.Errorf("fabriq: insights rollup query: %w", qerr)
						}
						maps, serr := scanMaps(rows)
						if serr != nil {
							return serr
						}
						return assignMapsDest(into, maps)
					})
				}
				// !hasWatermark: fall through to the live path below.
			}
		}
	}

	return i.a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
		sql, args, err := buildInsightsSQL(q, tid, d)
		if err != nil {
			return err
		}
		rows, qerr := tx.Query(ctx, sql, args...)
		if qerr != nil {
			return fmt.Errorf("fabriq: insights query: %w", qerr)
		}
		maps, serr := scanMaps(rows)
		if serr != nil {
			return serr
		}
		return assignMapsDest(into, maps)
	})
}

// QueryRaw implements query.AnalyticsQuerier — the read-only SQL escape hatch
// for aggregations the cube (Query, above) can't express. Modeled on
// (*Adapter).QueryDynamicReadOnly (adapter.go:582-617), the exact same
// tenant-stamped READ ONLY transaction pattern: Postgres itself rejects any
// write/DDL in a read-only tx, and RLS contains the reads to the caller's
// tenant. precheckInsightsReadOnly runs first as a cheap, fail-fast guard —
// defense-in-depth on top of the transaction, not a substitute for it.
//
// This lives on *InsightsAdapter, not *Adapter, for the same reason Query
// does: *Adapter already has a Query method for query.RelationalQuerier with
// a different signature. QueryRaw has no such collision (RelationalQuerier
// has no QueryRaw), but it is kept here so all three AnalyticsQuerier methods
// live together on one receiver type.
func (i *InsightsAdapter) QueryRaw(ctx context.Context, into any, sql string, args ...any) error {
	if err := precheckInsightsReadOnly(sql); err != nil {
		return err
	}
	if err := i.a.backstop.guardRawSQL(sql); err != nil {
		return err
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	tx, err := i.a.pg.BeginTx(ctx, &driver.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("fabriq: insights raw begin ro tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// SET and set_config are permitted in a read-only transaction; only data
	// writes are blocked.
	if _, err = tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tid); err != nil {
		return fmt.Errorf("fabriq: stamp tenant: %w", err)
	}
	if _, err = tx.Exec(ctx, `SELECT set_config('app.scope_id', $1, true)`, tenant.ScopeOrEmpty(ctx)); err != nil {
		return fmt.Errorf("fabriq: stamp scope: %w", err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = '%dms'`, RawQueryTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("fabriq: set timeout: %w", err)
	}

	rows, qerr := tx.Query(ctx, sql, args...)
	if qerr != nil {
		return classifyQueryErr(qerr)
	}
	maps, _, _, serr := scanMapsCapped(rows, maxRawQueryRows)
	if serr != nil {
		return classifyQueryErr(serr)
	}
	return assignMapsDest(into, maps)
}

// var _ insights.FactSink asserts *InsightsAdapter also implements the
// proj:insights consumer's write port, alongside query.AnalyticsQuerier
// above — both live on this receiver for cohesion.
var _ insights.FactSink = (*InsightsAdapter)(nil)

// UpsertInsightFacts implements insights.FactSink — version-gated upsert of
// projected domain facts into the tenant's own fabriq_insights_facts table.
// Goes through i.a.inTenantTx (not the dynamic-tx path Query/QueryRaw use)
// so RLS contains the write to the caller's tenant, mirroring the operator
// sink's UpsertFacts (adapters/pganalytics/sink.go) but tenant-scoped: the
// tenant is derived from ctx (via inTenantTx), not carried per-fact.
func (i *InsightsAdapter) UpsertInsightFacts(ctx context.Context, facts []insights.Fact) error {
	if len(facts) == 0 {
		return nil
	}
	return i.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		for _, f := range facts {
			const q = `INSERT INTO fabriq_insights_facts
				(tenant_id, scope_id, entity, agg_id, version, payload, at, deleted)
				VALUES ($1, NULLIF($2,''), $3, $4, $5, $6::jsonb, $7, $8)
				ON CONFLICT (tenant_id, entity, agg_id) DO UPDATE
				SET version = EXCLUDED.version, payload = EXCLUDED.payload,
				    at = EXCLUDED.at, deleted = EXCLUDED.deleted
				WHERE EXCLUDED.version > fabriq_insights_facts.version`
			if _, err := tx.NewRaw(q, f.TenantID, tenant.ScopeOrEmpty(ctx), f.Entity, f.AggID,
				f.Version, string(f.Payload), f.At, f.Deleted).Exec(ctx); err != nil {
				return fmt.Errorf("fabriq: insights upsert fact: %w", err)
			}
		}
		return nil
	})
}
