//go:build !duckdb

package fabriq

import (
	"context"
	"strings"
	"testing"
)

func TestOpenAnalyticsDuckDBNotBuilt(t *testing.T) {
	cfg := Config{Analytics: AnalyticsConfig{DSN: "duckdb:///tmp/a.db"}}
	err := openAnalytics(context.Background(), cfg, registryForTest(t), &Stores{})
	if err == nil || !strings.Contains(err.Error(), "duckdb analytics support not built") {
		t.Fatalf("want duckdb-not-built error, got %v", err)
	}
}
