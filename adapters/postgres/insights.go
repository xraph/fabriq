package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

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
// QueryRaw (the read-only SQL escape hatch, Task 8) is not implemented yet,
// so InsightsAdapter does not yet satisfy query.AnalyticsQuerier in full —
// intentionally no `var _ query.AnalyticsQuerier = (*InsightsAdapter)(nil)`
// assertion here; add it once QueryRaw lands.
type InsightsAdapter struct {
	a *Adapter
}

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
	return i.a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
		sql, args, err := buildInsightsSQL(q, tid)
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
