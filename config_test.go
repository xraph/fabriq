package fabriq

import "testing"

func TestConfigValidate_CatalogMode(t *testing.T) {
	base := Config{
		Catalog: CatalogConfig{
			DSN:         "postgres://control/db",
			ClusterDSNs: map[string]string{"c1": "postgres://pg-a:5432"},
		},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid catalog config rejected: %v", err)
	}

	withShards := base
	withShards.Shards = []ShardConfig{{ID: "a", DSN: "postgres://x"}}
	if err := withShards.Validate(); err == nil {
		t.Fatal("catalog + shards must be rejected (one routing authority)")
	}

	withPins := base
	withPins.ShardPins = map[string]string{"t1": "a"}
	if err := withPins.Validate(); err == nil {
		t.Fatal("catalog + shardPins must be rejected")
	}

	noClusters := base
	noClusters.Catalog.ClusterDSNs = nil
	if err := noClusters.Validate(); err == nil {
		t.Fatal("catalog mode without clusterDsns must be rejected")
	}

	emptyEntry := base
	emptyEntry.Catalog.ClusterDSNs = map[string]string{"": "postgres://x"}
	if err := emptyEntry.Validate(); err == nil {
		t.Fatal("empty cluster id must be rejected")
	}

	withPrimary := base
	withPrimary.Postgres.DSN = "postgres://primary/db"
	if err := withPrimary.Validate(); err == nil {
		t.Fatal("catalog + postgres.dsn must be rejected (ambiguous source of truth)")
	}
}
