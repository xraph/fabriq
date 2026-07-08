package fabriq

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func registryForTest(t *testing.T) *registry.Registry {
	t.Helper()
	return registry.New()
}

func TestAnalyticsScheme(t *testing.T) {
	cases := map[string]string{
		"clickhouse://h:9000/db": "clickhouse",
		"postgres://u@h/db":      "postgres",
		"postgresql://u@h/db":    "postgresql",
		"duckdb:///data/a.db":    "duckdb",
		"host=h dbname=fabriq":   "",
		"":                       "",
	}
	for dsn, want := range cases {
		if got := analyticsScheme(dsn); got != want {
			t.Errorf("analyticsScheme(%q) = %q, want %q", dsn, got, want)
		}
	}
}

func TestOpenAnalyticsUnknownScheme(t *testing.T) {
	cfg := Config{Analytics: AnalyticsConfig{DSN: "bogus://x"}}
	err := openAnalytics(context.Background(), cfg, registryForTest(t), &Stores{})
	if err == nil || !strings.Contains(err.Error(), "unknown analytics DSN scheme") {
		t.Fatalf("want unknown-scheme error, got %v", err)
	}
}

func TestOpenAnalyticsPartitionNonPostgres(t *testing.T) {
	cfg := Config{Analytics: AnalyticsConfig{DSN: "clickhouse://h:9000/db", PartitionEvents: true}}
	err := openAnalytics(context.Background(), cfg, registryForTest(t), &Stores{})
	if err == nil || !strings.Contains(err.Error(), "PartitionEvents") {
		t.Fatalf("want PartitionEvents-non-postgres error, got %v", err)
	}
}
