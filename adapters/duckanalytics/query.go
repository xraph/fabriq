//go:build duckdb

package duckanalytics

import (
	"context"
	"database/sql"
	"fmt"
)

// maxAnalyticsQueryRows caps a QueryReadOnly result set. A var (not const) so
// tests can shrink it to exercise truncation.
var maxAnalyticsQueryRows = 1000

// SetMaxAnalyticsQueryRowsForTest overrides the row cap. Test-only.
func SetMaxAnalyticsQueryRowsForTest(n int) { maxAnalyticsQueryRows = n }

// QueryReadOnly runs a read-only query (already validated read-only by the
// adminapi caller) against the DuckDB analytics store and returns a dynamic
// result set. DuckDB on a read-write handle has no per-statement read-only tx;
// read-only-ness is the caller's precheck + the ctx timeout + the row cap.
// File-access table functions (read_csv/read_parquet/read_text/parquet_scan/
// glob/etc.) are blocked at the adminapi precheck, not here; the pg adapter
// has no such exposure since it has no local filesystem to read from.
func (s *Sink) QueryReadOnly(ctx context.Context, query string, args ...any) (rows []map[string]any, cols []string, truncated bool, err error) {
	r, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, false, err
	}
	return scanSQLMapsCapped(r, maxAnalyticsQueryRows)
}

// scanSQLMapsCapped drains up to limit rows into maps; if more remain it stops
// and reports truncated. []byte cells are surfaced as strings so JSON payload
// columns serialize as text.
func scanSQLMapsCapped(r *sql.Rows, limit int) (out []map[string]any, cols []string, truncated bool, err error) {
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
