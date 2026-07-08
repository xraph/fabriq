//go:build !duckdb

package fabriq

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/analytics"
)

// openDuckAnalytics is the default (no-CGO) stub. The embedded-DuckDB adapter is
// compiled only with -tags duckdb (see adapters/duckanalytics).
func openDuckAnalytics(_ context.Context, _ string) (analytics.Sink, error) {
	return nil, fmt.Errorf("fabriq: duckdb analytics support not built into this binary (rebuild with -tags duckdb)")
}
