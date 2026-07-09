//go:build duckdb

package main

import "testing"

// With -tags duckdb, the demo defaults to an embedded-DuckDB file under dataDir.
func TestDefaultAnalyticsDSN_DuckDB(t *testing.T) {
	got := defaultAnalyticsDSN("/tmp/demo")
	want := "duckdb:///tmp/demo/analytics.duckdb"
	if got != want {
		t.Fatalf("defaultAnalyticsDSN(-tags duckdb) = %q, want %q", got, want)
	}
}
