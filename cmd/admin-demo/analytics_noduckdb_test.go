//go:build !duckdb

package main

import "testing"

// Without the duckdb tag, the seam returns "" so fabriq.Open runs with analytics
// disabled rather than failing on the "duckdb analytics support not built" stub.
func TestDefaultAnalyticsDSN_NoDuckDB(t *testing.T) {
	if got := defaultAnalyticsDSN("/tmp/demo"); got != "" {
		t.Fatalf("defaultAnalyticsDSN(untagged) = %q, want \"\"", got)
	}
}
