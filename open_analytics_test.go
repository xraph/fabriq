package fabriq

import "testing"

func TestAnalyticsConfig_EnabledByDSN(t *testing.T) {
	if (AnalyticsConfig{}).Enabled() {
		t.Fatal("empty DSN must be disabled")
	}
	if !(AnalyticsConfig{DSN: "postgres://x"}).Enabled() {
		t.Fatal("DSN present must be enabled")
	}
}

func TestOpen_AnalyticsDSNCollisionRejected(t *testing.T) {
	collideWithPostgres := Config{
		Postgres:  PostgresConfig{DSN: "postgres://same"},
		Analytics: AnalyticsConfig{DSN: "postgres://same"},
	}
	if err := ValidateAnalyticsConfig(collideWithPostgres); err == nil {
		t.Fatal("expected error when analytics DSN collides with postgres.dsn")
	}

	collideWithCatalog := Config{
		Catalog:   CatalogConfig{DSN: "postgres://catalog"},
		Analytics: AnalyticsConfig{DSN: "postgres://catalog"},
	}
	if err := ValidateAnalyticsConfig(collideWithCatalog); err == nil {
		t.Fatal("expected error when analytics DSN collides with catalog.dsn")
	}

	collideWithShard := Config{
		Shards:    []ShardConfig{{ID: "s1", DSN: "postgres://shard1"}},
		Analytics: AnalyticsConfig{DSN: "postgres://shard1"},
	}
	if err := ValidateAnalyticsConfig(collideWithShard); err == nil {
		t.Fatal("expected error when analytics DSN collides with a shard DSN")
	}

	collideWithCatalogCluster := Config{
		Catalog:   CatalogConfig{ClusterDSNs: map[string]string{"c1": "postgres://x/db"}},
		Analytics: AnalyticsConfig{DSN: "postgres://x/db"},
	}
	if err := ValidateAnalyticsConfig(collideWithCatalogCluster); err == nil {
		t.Fatal("expected error when analytics DSN collides with a catalog cluster DSN")
	}

	distinctCatalogCluster := Config{
		Catalog:   CatalogConfig{ClusterDSNs: map[string]string{"c1": "postgres://x/db"}},
		Analytics: AnalyticsConfig{DSN: "postgres://analytics-only"},
	}
	if err := ValidateAnalyticsConfig(distinctCatalogCluster); err != nil {
		t.Fatalf("expected no error for a distinct catalog cluster DSN, got %v", err)
	}

	distinct := Config{
		Postgres:  PostgresConfig{DSN: "postgres://tenant"},
		Catalog:   CatalogConfig{DSN: "postgres://catalog"},
		Shards:    []ShardConfig{{ID: "s1", DSN: "postgres://shard1"}},
		Analytics: AnalyticsConfig{DSN: "postgres://analytics"},
	}
	if err := ValidateAnalyticsConfig(distinct); err != nil {
		t.Fatalf("expected no error for distinct DSNs, got %v", err)
	}

	disabled := Config{
		Postgres: PostgresConfig{DSN: "postgres://same"},
	}
	disabled.Analytics.DSN = ""
	if err := ValidateAnalyticsConfig(disabled); err != nil {
		t.Fatalf("expected no error when analytics is disabled, got %v", err)
	}
}
