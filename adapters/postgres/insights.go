package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
