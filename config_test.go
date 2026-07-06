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

func TestConfig_AdaptiveValidates(t *testing.T) {
	c := Config{Catalog: CatalogConfig{
		DSN:         "postgres://control",
		ClusterDSNs: map[string]string{"c1": "postgres://c1"},
		Adaptive:    AdaptivePoolConfig{Enabled: true, Min: 8, Max: 64},
	}}
	if err := c.Validate(); err != nil {
		t.Fatalf("valid adaptive config rejected: %v", err)
	}
}

func TestConfig_AdaptiveRejectsMinZero(t *testing.T) {
	c := Config{Catalog: CatalogConfig{
		DSN:         "postgres://control",
		ClusterDSNs: map[string]string{"c1": "postgres://c1"},
		Adaptive:    AdaptivePoolConfig{Enabled: true, Min: 0, Max: 64},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("adaptive with min<1 must be rejected")
	}
}

func TestConfig_AdaptiveRejectsMaxBelowMin(t *testing.T) {
	c := Config{Catalog: CatalogConfig{
		DSN:         "postgres://control",
		ClusterDSNs: map[string]string{"c1": "postgres://c1"},
		Adaptive:    AdaptivePoolConfig{Enabled: true, Min: 64, Max: 8},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("adaptive with max<min must be rejected")
	}
}

func TestAdaptiveConfig_AppliesDefaults(t *testing.T) {
	cc := CatalogConfig{Adaptive: AdaptivePoolConfig{Enabled: true}}
	ac := adaptiveConfig(cc, 100)
	if ac == nil {
		t.Fatal("adaptiveConfig returned nil for enabled config")
	}
	if ac.Min != 8 {
		t.Errorf("Min=%d want default 8", ac.Min)
	}
	if ac.Max != 100 { // defaults to the static MaxActiveShards
		t.Errorf("Max=%d want static 100", ac.Max)
	}
}

func TestAdaptiveConfig_DisabledIsNil(t *testing.T) {
	if adaptiveConfig(CatalogConfig{}, 100) != nil {
		t.Fatal("disabled adaptive must map to nil")
	}
}
