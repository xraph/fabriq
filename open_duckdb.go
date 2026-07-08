//go:build duckdb

package fabriq

import (
	"context"

	"github.com/xraph/fabriq/adapters/duckanalytics"
	"github.com/xraph/fabriq/core/analytics"
)

// openDuckAnalytics dials the embedded DuckDB analytics sink. Compiled only with
// -tags duckdb; the default build uses the stub in open_noduckdb.go.
func openDuckAnalytics(ctx context.Context, dsn string) (analytics.Sink, error) {
	return duckanalytics.Open(ctx, dsn)
}
