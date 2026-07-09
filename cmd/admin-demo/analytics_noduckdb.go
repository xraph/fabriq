//go:build !duckdb

package main

// defaultAnalyticsDSN is the default (no-CGO) stub: analytics is OFF unless the
// operator sets ADMIN_DEMO_ANALYTICS_DSN to a non-DuckDB backend (e.g.
// postgres://…). The embedded-DuckDB sink is compiled in only with -tags duckdb
// (see analytics_duckdb.go and adapters/duckanalytics), which is where the demo
// gets a default columnar backend without any external service.
func defaultAnalyticsDSN(_ string) string { return "" }
