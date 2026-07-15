package postgres

// The rollup maintainer (phase 2b, Task 4): given a tenant-stamped ctx and a
// materialized MetricSpec, aggregates the metric's sealed time-buckets from
// fabriq_insights_events into the metric's per-metric rollup table
// (fabriq_insights_rollup_<metric>, built by Task 3's EnsureRollupTable),
// incrementally by watermark (fabriq_insights_rollup_state, Task 2's
// migration), re-rolling a trailing window to absorb late arrivals.
//
// Tenant enumeration (looping over every tenant that has this metric's data)
// is explicitly NOT this file's job — see the design's "The maintainer"
// section and the task-4 brief: the caller (Task 6's orchestration/worker
// layer) supplies one tenant-stamped ctx per pass. Every method here runs
// inside inTenantTx (or builds SQL for a caller that does), the same seam
// Track/UpsertInsightFacts use, so RLS contains reads/writes to the caller's
// tenant.
//
// A maintainer pass is deliberately run under an UNSCOPED tenant ctx (no
// app.scope_id stamped, or stamped empty) so its RLS predicate's first OR
// branch — `current_setting('app.scope_id', true) = ''` — is satisfied and
// it can see every scope's events within the tenant, not just one scope's.
// RollupRange's SELECT groups by the events' own scope_id (preserving NULL
// for "shared"), so per-scope aggregates land in their own rollup rows
// regardless of which scope wrote the source events.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// defaultSealGrace is used when a RollupSpec leaves SealGrace at its zero
// value: how long past a bucket's end time the maintainer waits before
// treating it as sealed (no longer mutable), so events that arrive slightly
// late still land in the correct bucket before it is rolled up.
const defaultSealGrace = 5 * time.Minute

// defaultRerollMultiple is used when a RollupSpec leaves RerollWindow at its
// zero value: the trailing re-roll window defaults to this many multiples of
// the metric's own Bucket.
const defaultRerollMultiple = 2

// ReadRollupWatermark reads the current (tenant, metric) rollup watermark —
// the newest bucket_start the maintainer has fully sealed and rolled up —
// from fabriq_insights_rollup_state. RLS scopes the read to the ctx's
// tenant; the caller (an unscoped maintainer pass) sees the one row for
// (tenant, metric) regardless of that row's own scope_id (the PK is
// (tenant_id, metric), so there is at most one).
//
// Returns (zero time, false, nil) when no watermark row exists yet — the
// metric's first maintainer pass — rather than an error: "no watermark yet"
// is an expected, ordinary state, not a failure.
//
// Uses inDynamicTenantTx (a raw driver.Tx), not inTenantTx's *pgdriver.PgTx:
// *pgdriver.PgTx's only scalar-read path is RawQuery.Scan, which
// mis-dispatches a lone struct-kind destination like *time.Time into its
// ORM-style struct-scan branch (resolveTable "succeeds" against time.Time
// with zero mapped fields, then errors with a field-count mismatch) instead
// of the plain scalar Scan every other single-column read here needs.
// driver.Tx's QueryRow(...).Scan(...) goes straight to the underlying pgx
// row, exactly like InsightsAdapter.QueryRaw and Query already use it.
func (a *Adapter) ReadRollupWatermark(ctx context.Context, metric string) (time.Time, bool, error) {
	var wm time.Time
	found := false
	err := a.inDynamicTenantTx(ctx, func(_ string, tx driver.Tx) error {
		row := tx.QueryRow(ctx, `SELECT watermark_bucket FROM fabriq_insights_rollup_state WHERE metric = $1`, metric)
		serr := row.Scan(&wm)
		if serr != nil {
			if isNoRows(serr) {
				return nil
			}
			return fmt.Errorf("fabriq: read rollup watermark for metric %q: %w", metric, serr)
		}
		found = true
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return wm, found, nil
}

// AdvanceRollupWatermark upserts the (tenant, metric) rollup watermark to
// bucket, taking the GREATEST of the existing and new values so a
// concurrent or out-of-order call can never move the watermark backwards.
// scope_id is stored NULL (via NULLIF($2,”)) — the watermark is per
// (tenant, metric), not per scope (rollup_state's PK is (tenant_id, metric));
// scope_id on this table is incidental, carried only for the same
// scope-aware-RLS shape the sibling insights tables share.
func (a *Adapter) AdvanceRollupWatermark(ctx context.Context, metric string, bucket time.Time) error {
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		const q = `INSERT INTO fabriq_insights_rollup_state (tenant_id, scope_id, metric, watermark_bucket)
			VALUES ($1, NULLIF($2, ''), $3, $4)
			ON CONFLICT (tenant_id, metric) DO UPDATE
			SET watermark_bucket = GREATEST(fabriq_insights_rollup_state.watermark_bucket, EXCLUDED.watermark_bucket)`
		if _, err := tx.NewRaw(q, tid, tenant.ScopeOrEmpty(ctx), metric, bucket).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: advance rollup watermark for metric %q: %w", metric, err)
		}
		return nil
	})
}

// rollupMeasureSelect renders one measure's INSERT-column name(s) and
// SELECT-list aggregate expression(s) for RollupRange, mirroring
// rollupMeasureColumns' (insights_rollup_ddl.go) column shape exactly: count/
// sum/min/max map to one column, avg decomposes into "<alias>__sum" +
// "<alias>__count" (additive storage — the maintainer/router recompute
// avg = sum/count when reading). cols and updateCols are appended to (not
// returned fresh) so the caller can build one running list across all
// measures.
func rollupMeasureSelect(m registry.MetricMeasure, insertCols, selects, updateCols *[]string) error {
	switch m.Kind {
	case "count":
		alias := rollupMeasureAlias(m)
		if !ddlValid(alias) {
			return fmt.Errorf("invalid measure column name %q (kind %q)", alias, m.Kind)
		}
		*insertCols = append(*insertCols, alias)
		*selects = append(*selects, "count(*)")
		*updateCols = append(*updateCols, alias)
		return nil
	case "sum", "min", "max":
		alias := rollupMeasureAlias(m)
		if !ddlValid(alias) {
			return fmt.Errorf("invalid measure column name %q (kind %q)", alias, m.Kind)
		}
		acc, err := propAccessor("props", m.Field)
		if err != nil {
			return err
		}
		fn := map[string]string{"sum": "SUM", "min": "MIN", "max": "MAX"}[m.Kind]
		*insertCols = append(*insertCols, alias)
		*selects = append(*selects, fmt.Sprintf("%s((%s)::numeric)", fn, acc))
		*updateCols = append(*updateCols, alias)
		return nil
	case "avg":
		alias := rollupMeasureAlias(m)
		sumCol := alias + "__sum"
		countCol := alias + "__count"
		if !ddlValid(sumCol) || !ddlValid(countCol) {
			return fmt.Errorf("invalid decomposed avg column for alias %q", alias)
		}
		acc, err := propAccessor("props", m.Field)
		if err != nil {
			return err
		}
		*insertCols = append(*insertCols, sumCol, countCol)
		*selects = append(*selects, fmt.Sprintf("SUM((%s)::numeric)", acc), fmt.Sprintf("COUNT(%s)", acc))
		*updateCols = append(*updateCols, sumCol, countCol)
		return nil
	case "count_distinct", "percentile":
		return fmt.Errorf("measure kind %q is not additive — rollups do not support sketch measures until phase 2b-2", m.Kind)
	default:
		return fmt.Errorf("unknown measure kind %q", m.Kind)
	}
}

// buildRollupRangeSQL builds the aggregate-and-upsert SQL and its positional
// args for RollupRange, scoped to tenant tid. $1 = tenant_id, $2 = the rollup
// grain as a "N seconds" interval literal, $3 = m.Source (the event name),
// $4/$5 = the [from, to) bucket range.
//
// Dimensions and measures are SELECTed via propAccessor's `props ->> 'key'`
// idiom, the same accessor insights_query_build.go's live cube path uses,
// grouped by scope_id (preserving NULL — never coalesced, so the unique
// index's NULLS NOT DISTINCT still upserts unscoped rows onto one row) and
// the bucket/dimension expressions (not their aliases, matching
// buildInsightsSQL's own convention: Postgres GROUP BY accepts either, but
// repeating the expression avoids relying on alias-in-GROUP-BY semantics).
//
// Every interpolated identifier (dimension names, measure alias/columns) is
// ddlValid/insightsIdentRe-checked before being placed in the SQL text —
// this function's own injection-guard boundary, independent of but
// consistent with rollupTableDDL's.
func buildRollupRangeSQL(m *registry.MetricSpec, tid string, grain string, from, to time.Time) (sql string, args []any, err error) {
	table, err := rollupTableName(m.Name)
	if err != nil {
		return "", nil, err
	}

	insertCols := []string{"tenant_id", "scope_id", "bucket_start"}
	selects := []string{"$1", "scope_id", "time_bucket($2::interval, at)"}
	groups := []string{"scope_id", "time_bucket($2::interval, at)"}
	conflictCols := []string{"tenant_id", "scope_id", "bucket_start"}

	for _, dim := range m.Dimensions {
		if !ddlValid(dim) {
			return "", nil, fmt.Errorf("fabriq: metric %q: invalid rollup dimension name %q", m.Name, dim)
		}
		acc, aerr := propAccessor("props", dim)
		if aerr != nil {
			return "", nil, aerr
		}
		insertCols = append(insertCols, dim)
		selects = append(selects, acc)
		groups = append(groups, acc)
		conflictCols = append(conflictCols, dim)
	}

	var updateCols []string
	for _, meas := range m.Measures {
		if merr := rollupMeasureSelect(meas, &insertCols, &selects, &updateCols); merr != nil {
			return "", nil, fmt.Errorf("fabriq: metric %q: %w", m.Name, merr)
		}
	}
	if len(updateCols) == 0 {
		return "", nil, fmt.Errorf("fabriq: metric %q has no measures to roll up", m.Name)
	}

	setParts := make([]string, 0, len(updateCols))
	for _, c := range updateCols {
		setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "INSERT INTO %s (%s)\nSELECT %s\nFROM fabriq_insights_events\nWHERE tenant_id = $1 AND name = $3 AND at >= $4 AND at < $5\nGROUP BY %s\nON CONFLICT (%s) DO UPDATE SET %s",
		table,
		strings.Join(insertCols, ", "),
		strings.Join(selects, ", "),
		strings.Join(groups, ", "),
		strings.Join(conflictCols, ", "),
		strings.Join(setParts, ", "),
	)

	args = []any{tid, grain, m.Source, from, to}
	return sb.String(), args, nil
}

// RollupRange aggregates m's sealed events in [from, to) — grouped by
// scope_id (NULL preserved), the metric's declared Bucket, and its
// Dimensions — and upserts the result into m's rollup table, overwriting any
// existing row for the same (tenant, scope, bucket, dims) key (ON CONFLICT
// DO UPDATE). Idempotent: re-running it for the same range recomputes and
// overwrites the same rows with the same values.
//
// Runs inside inTenantTx: the caller's ctx supplies the tenant (and should be
// unscoped — see this file's header comment — so the aggregation sees every
// scope's events, not just one).
func (a *Adapter) RollupRange(ctx context.Context, m *registry.MetricSpec, from, to time.Time) error {
	if m.Rollup == nil || m.Rollup.Bucket <= 0 {
		return fmt.Errorf("fabriq: metric %q has no rollup bucket configured", m.Name)
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	grain := fmt.Sprintf("%d seconds", int64(m.Rollup.Bucket/time.Second))
	sql, args, err := buildRollupRangeSQL(m, tid, grain, from, to)
	if err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		if _, err := tx.NewRaw(sql, args...).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: rollup range for metric %q [%s, %s): %w", m.Name, from, to, err)
		}
		return nil
	})
}

// MaintainRollup runs one maintainer pass for m: seals buckets whose end has
// passed SealGrace, aggregates the sealable range (plus a trailing
// RerollWindow re-roll to absorb late arrivals into already-sealed buckets),
// and advances the watermark. A no-op (returns nil without touching the
// rollup table or watermark) when nothing new has sealed since the last
// pass.
//
//   - sealBefore = now - SealGrace (SealGrace defaults to defaultSealGrace
//     when the metric leaves it at zero).
//   - to = sealBefore truncated to the metric's Bucket — the newest bucket
//     boundary that is fully sealed.
//   - from: on a metric with an existing watermark, `wm - RerollWindow`
//     (RerollWindow defaults to defaultRerollMultiple*Bucket when left at
//     zero) — re-rolling the trailing window absorbs events that arrived
//     late (within grace) but after an earlier pass already sealed that
//     bucket. On a metric's first pass (no watermark row yet), from is the
//     zero time.Time — a far-past floor — so the first pass rolls up every
//     historical sealed bucket.
//   - Guard: if to does not come strictly after from, nothing has newly
//     sealed since the last pass (or ever, on a fresh metric with no data
//     yet) — no-op, watermark untouched.
func (a *Adapter) MaintainRollup(ctx context.Context, m *registry.MetricSpec, now time.Time) error {
	if m.Rollup == nil || m.Rollup.Bucket <= 0 {
		return fmt.Errorf("fabriq: metric %q has no rollup bucket configured", m.Name)
	}

	grace := m.Rollup.SealGrace
	if grace <= 0 {
		grace = defaultSealGrace
	}
	reroll := m.Rollup.RerollWindow
	if reroll <= 0 {
		reroll = defaultRerollMultiple * m.Rollup.Bucket
	}

	sealBefore := now.Add(-grace)
	to := sealBefore.Truncate(m.Rollup.Bucket)

	wm, ok, err := a.ReadRollupWatermark(ctx, m.Name)
	if err != nil {
		return err
	}

	var from time.Time
	if ok {
		from = wm.Add(-reroll)
	}
	// !ok: from stays the zero time.Time — a far-past floor — so the first
	// pass for this metric rolls up every historical sealed bucket.

	if !to.After(from) {
		// Nothing newly sealed since the last pass.
		return nil
	}

	if err := a.RollupRange(ctx, m, from, to); err != nil {
		return err
	}
	return a.AdvanceRollupWatermark(ctx, m.Name, to)
}
