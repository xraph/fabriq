package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// seriesTables are the telemetry tables BulkWrite/Range may touch. The
// hypertable has no RLS (columnstore restriction), so the series name is
// validated here and tenancy is stamped structurally into every statement.
func validSeries(series string) error {
	if series == "" || strings.ContainsAny(series, `"; `) {
		return fmt.Errorf("fabriq: invalid series name %q", series)
	}
	return nil
}

// BulkWrite implements query.TSQuerier — the event-bypass telemetry path.
// One multi-row INSERT per call; no per-row outbox events (the worker
// publishes conflated deltas instead).
func (a *Adapter) BulkWrite(ctx context.Context, series string, points []query.Point) error {
	if err := validSeries(series); err != nil {
		return err
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var sb strings.Builder
		args := make([]any, 0, len(points)*4+1)
		args = append(args, tid)
		fmt.Fprintf(&sb, `INSERT INTO %s (time, tenant_id, tag_id, value, quality) VALUES `, quoteIdent(series))
		for i, p := range points {
			if i > 0 {
				sb.WriteByte(',')
			}
			n := len(args)
			fmt.Fprintf(&sb, "($%d, $1, $%d, $%d, $%d)", n+1, n+2, n+3, n+4)
			args = append(args, p.At, p.Key, p.Value, p.Quality)
		}
		if _, err := tx.NewRaw(sb.String(), args...).Exec(ctx); err != nil {
			return fmt.Errorf("fabriq: bulk write %d points to %s: %w", len(points), series, err)
		}
		return nil
	})
}

type pointRow struct {
	Key     string  `grove:"key"`
	At      string  `grove:"at"` // scanned as text, parsed below (tz-stable)
	Value   float64 `grove:"value"`
	Quality int     `grove:"quality"`
}

// Range implements query.TSQuerier for raw points over [From, To).
// Bucketed aggregates land with the projection phase (time_bucket).
func (a *Adapter) Range(ctx context.Context, q query.RangeQuery, into any) error {
	if err := validSeries(q.Series); err != nil {
		return err
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	dest, ok := into.(*[]query.Point)
	if !ok {
		return fmt.Errorf("fabriq: Range scans into *[]query.Point, got %T", into)
	}
	if q.Bucket > 0 {
		return fmt.Errorf("fabriq: bucketed Range is not implemented yet (phase 6)")
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var rows []pointRow
		sql := fmt.Sprintf(`SELECT tag_id AS key, time::text AS at, value, quality
			FROM %s WHERE tenant_id = $1 AND tag_id = $2 AND time >= $3 AND time < $4
			ORDER BY time ASC`, quoteIdent(q.Series))
		if err := tx.NewRaw(sql, tid, q.Key, q.From, q.To).Scan(ctx, &rows); err != nil {
			return fmt.Errorf("fabriq: range %s/%s: %w", q.Series, q.Key, err)
		}
		for _, r := range rows {
			at, err := parsePGTime(r.At)
			if err != nil {
				return err
			}
			*dest = append(*dest, query.Point{Key: r.Key, At: at, Value: r.Value, Quality: r.Quality})
		}
		return nil
	})
}
