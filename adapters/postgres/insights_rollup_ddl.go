package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/registry"
)

// rollupTablePrefix names the per-metric materialized-rollup tables (phase
// 2b). One table per materialized MetricSpec: fabriq_insights_rollup_<metric>.
const rollupTablePrefix = "fabriq_insights_rollup_"

// rollupTableName returns the physical table name for a materialized
// metric's rollup, validating the FULL name (prefix + metric) against
// ddlValid — this both rejects a metric name containing SQL-hostile
// characters (e.g. "bad-name", "x; DROP") and catches a name that would
// overflow Postgres's identifier length once the prefix is added.
func rollupTableName(metric string) (string, error) {
	if metric == "" {
		return "", fmt.Errorf("fabriq: rollup metric name must not be empty")
	}
	table := rollupTablePrefix + metric
	if !ddlValid(table) {
		return "", fmt.Errorf("fabriq: invalid rollup metric name %q (table name %q is not a valid identifier)", metric, table)
	}
	return table, nil
}

// rollupMeasureAlias returns the measure's column alias: its declared As, or
// a defaulted alias when As is empty — "count" for a count measure (which
// typically has no Field), "<kind>_<field>" otherwise. The caller
// ddlValid-checks the result before using it in DDL.
func rollupMeasureAlias(m registry.MetricMeasure) string {
	if m.As != "" {
		return m.As
	}
	if m.Kind == "count" {
		return "count"
	}
	return fmt.Sprintf("%s_%s", m.Kind, m.Field)
}

// rollupMeasureColumns returns the additive rollup column definition(s) for
// one measure. count/sum/min/max each map to a single NUMERIC column named by
// its alias. avg is decomposed into TWO NUMERIC columns, "<alias>__sum" and
// "<alias>__count", so the rollup stores sum-and-count rather than a
// non-additive average — the maintainer/router recompute avg = sum/count
// when combining sealed and live-tail partials (see the design's Storage +
// stitching-router sections). count_distinct/percentile ("sketch" measures)
// are NOT supported by 2b-1's additive-only rollup storage — registry.Validate
// already rejects them on a Rollup-opted metric, but this is checked
// defensively here too since rollupTableDDL is also a direct, testable
// injection-guard boundary independent of the registry.
//
// Every interpolated identifier (the alias, and its decomposed avg
// variants) is ddlValid-checked before being returned — the caller must
// treat a returned error as "do not build this table."
func rollupMeasureColumns(m registry.MetricMeasure) ([]string, error) {
	switch m.Kind {
	case "count", "sum", "min", "max":
		alias := rollupMeasureAlias(m)
		if !ddlValid(alias) {
			return nil, fmt.Errorf("invalid measure column name %q (kind %q)", alias, m.Kind)
		}
		return []string{fmt.Sprintf("%s NUMERIC", alias)}, nil
	case "avg":
		alias := rollupMeasureAlias(m)
		sumCol := alias + "__sum"
		countCol := alias + "__count"
		if !ddlValid(sumCol) || !ddlValid(countCol) {
			return nil, fmt.Errorf("invalid decomposed avg column for alias %q", alias)
		}
		return []string{
			fmt.Sprintf("%s NUMERIC", sumCol),
			fmt.Sprintf("%s NUMERIC", countCol),
		}, nil
	case "count_distinct", "percentile":
		// Sketch measures require timescaledb_toolkit storage (hyperloglog/
		// tdigest columns), which 2b-1 does not build. registry.Validate
		// already rejects a Rollup metric with a sketch measure before this
		// is ever reached in production; this is a defensive second guard.
		return nil, fmt.Errorf("measure kind %q is not additive — rollups do not support sketch measures until phase 2b-2", m.Kind)
	default:
		return nil, fmt.Errorf("unknown measure kind %q", m.Kind)
	}
}

// rollupRLSStatements returns the tenant-isolation RLS statements for a
// runtime-created rollup table. This deliberately mirrors EnsureDynamic's
// exact runtime RLS pattern in ddl.go (ENABLE/FORCE ROW LEVEL SECURITY +
// DROP POLICY IF EXISTS + CREATE POLICY tenant_isolation), NOT
// migrations.ScopeAwareTenantPolicy: ddl.go is the runtime managed-DDL seam
// (this file's sibling), while ScopeAwareTenantPolicy is the migrations
// package's boot-time-schema equivalent — reusing the latter here would pull
// adapters/postgres's runtime DDL lane into a dependency on the migrations
// package for no behavioral gain.
//
// NOTE (see task-3-report.md "concerns" for the full writeup): the rollup
// table also carries a scope_id column (per the design's Storage section),
// but ddl.go's runtime pattern predates any runtime table having scope_id —
// EnsureDynamic's dynamic-entity tables never carry one — so its policy
// predicate is tenant-only. Copying it verbatim, as directed, means scope_id
// is stored as data but is NOT part of the RLS predicate on this table
// (unlike the migration-created fabriq_insights_events/facts/rollup_state
// tables, which DO use the scope-aware predicate). A tenant's own rows are
// still fully isolated from other tenants either way.
func rollupRLSStatements(table string) []string {
	return []string{
		fmt.Sprintf(`ALTER TABLE %s ENABLE ROW LEVEL SECURITY`, table),
		fmt.Sprintf(`ALTER TABLE %s FORCE ROW LEVEL SECURITY`, table),
		fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, table),
		fmt.Sprintf(`CREATE POLICY tenant_isolation ON %s USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`, table),
	}
}

// rollupTableDDL builds the statements to create (idempotently) the
// per-metric materialized-rollup table for m: tenant_id/scope_id/bucket_start
// structural columns, one TEXT column per rollup dimension (m.Dimensions),
// one or two NUMERIC columns per additive measure (rollupMeasureColumns), a
// composite primary key over (tenant_id, scope_id, bucket_start, <dims...>),
// and the runtime RLS statements (rollupRLSStatements).
//
// Pure: no I/O, no DB. Every interpolated identifier — the table name, each
// dimension column, each measure column — is ddlValid-checked before use;
// any invalid name is returned as an error rather than silently ignored,
// so this is the injection-guard boundary for rollup DDL.
func rollupTableDDL(m *registry.MetricSpec) ([]string, error) {
	table, err := rollupTableName(m.Name)
	if err != nil {
		return nil, err
	}

	cols := []string{
		"tenant_id TEXT NOT NULL",
		"scope_id TEXT",
		"bucket_start TIMESTAMPTZ NOT NULL",
	}
	pk := []string{"tenant_id", "scope_id", "bucket_start"}

	for _, dim := range m.Dimensions {
		if !ddlValid(dim) {
			return nil, fmt.Errorf("fabriq: metric %q: invalid rollup dimension name %q", m.Name, dim)
		}
		cols = append(cols, fmt.Sprintf("%s TEXT", dim))
		pk = append(pk, dim)
	}

	for _, meas := range m.Measures {
		measureCols, err := rollupMeasureColumns(meas)
		if err != nil {
			return nil, fmt.Errorf("fabriq: metric %q: %w", m.Name, err)
		}
		cols = append(cols, measureCols...)
	}

	create := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n\t%s,\n\tPRIMARY KEY (%s)\n)",
		table, strings.Join(cols, ",\n\t"), strings.Join(pk, ", "),
	)

	stmts := append([]string{create}, rollupRLSStatements(table)...)
	return stmts, nil
}

// EnsureRollupTable creates (idempotently) the per-metric materialized-
// rollup table for m, via the same owner/DDL exec seam EnsureDynamic uses
// (a.execDDL — the schema-owner pool path, NOT a tenant-scoped transaction:
// DDL is tenant-agnostic; the table's RLS policy enforces per-row tenant
// isolation once rows are written). Safe to call repeatedly (CREATE TABLE
// IF NOT EXISTS + idempotent RLS statements).
func (a *Adapter) EnsureRollupTable(ctx context.Context, m *registry.MetricSpec) error {
	stmts, err := rollupTableDDL(m)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if err := a.execDDL(ctx, stmt); err != nil {
			return fmt.Errorf("fabriq: ensure rollup table for metric %q: %w", m.Name, err)
		}
	}
	return nil
}
