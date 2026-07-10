package pganalytics

import (
	"context"
	"fmt"

	"github.com/xraph/grove/driver"
)

// maxAnalyticsQueryRows caps a QueryReadOnly result set. A var so tests can
// shrink it to exercise truncation.
var maxAnalyticsQueryRows = 1000

// QueryReadOnly runs a query inside a READ ONLY transaction against the
// Postgres analytics store and returns a dynamic result set. The read-only tx
// is the REAL enforcement — a data-modifying statement (including a
// data-modifying CTE) errors at the database, regardless of the caller's
// precheck. The row cap bounds the result; the caller bounds time via ctx.
func (s *Sink) QueryReadOnly(ctx context.Context, query string, args ...any) (rows []map[string]any, cols []string, truncated bool, err error) {
	tx, err := s.db.BeginTx(ctx, &driver.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: analytics query begin ro tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	r, qerr := tx.Query(ctx, query, args...)
	if qerr != nil {
		return nil, nil, false, qerr
	}
	return scanDriverMapsCapped(r, maxAnalyticsQueryRows)
}

// scanDriverMapsCapped drains up to limit rows from a grove driver.Rows into
// maps; if more remain it stops and reports truncated. []byte cells surface as
// strings so JSON payload columns serialize as text.
func scanDriverMapsCapped(r driver.Rows, limit int) (out []map[string]any, cols []string, truncated bool, err error) {
	defer func() { _ = r.Close() }()
	cols, err = r.Columns()
	if err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: analytics query columns: %w", err)
	}
	for r.Next() {
		if len(out) >= limit {
			truncated = true
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cols {
			ptrs[i] = &vals[i]
		}
		if serr := r.Scan(ptrs...); serr != nil {
			return nil, nil, false, fmt.Errorf("fabriq: analytics query scan: %w", serr)
		}
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			m[col] = v
		}
		out = append(out, m)
	}
	if rerr := r.Err(); rerr != nil {
		return nil, nil, false, fmt.Errorf("fabriq: analytics query rows: %w", rerr)
	}
	return out, cols, truncated, nil
}
