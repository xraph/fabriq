//go:build duckdb

package main

import "path/filepath"

// defaultAnalyticsDSN returns the embedded-DuckDB analytics DSN for the demo:
// a FILE under dataDir (not duckdb://:memory:), so the columnar read model
// persists across restarts and survives the demo's pooled database/sql handle —
// a shared in-memory DuckDB can otherwise hand out per-connection independent
// instances. Compiled only with -tags duckdb (CGO); the untagged build uses the
// stub in analytics_noduckdb.go, which returns "" (analytics off).
func defaultAnalyticsDSN(dataDir string) string {
	return "duckdb://" + filepath.Join(dataDir, "analytics.duckdb")
}
