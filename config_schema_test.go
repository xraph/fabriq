package fabriq

import "testing"

func schemaCfg(iso, shared string) Config {
	return Config{Catalog: CatalogConfig{
		DSN:         "postgres://h/ctl",
		ClusterDSNs: map[string]string{"c1": "postgres://h/postgres"},
		Isolation:   iso, SharedSchema: shared,
	}}
}

func TestCatalogConfig_IsolationMustBeKnown(t *testing.T) {
	if err := schemaCfg("weird", "").Validate(); err == nil {
		t.Fatal("expected unknown isolation rejection")
	}
	for _, ok := range []string{"", "database", "schema"} {
		if err := schemaCfg(ok, "").Validate(); err != nil {
			t.Fatalf("isolation %q rejected: %v", ok, err)
		}
	}
}

func TestCatalogConfig_SchemaModeRejectsBadSharedSchema(t *testing.T) {
	if err := schemaCfg("schema", "Bad Schema").Validate(); err == nil {
		t.Fatal("expected invalid shared schema rejection")
	}
}

func TestCatalogConfig_SchemaModeDefaultsSharedSchema(t *testing.T) {
	c := schemaCfg("schema", "")
	if !c.Catalog.SchemaMode() || c.Catalog.sharedSchemaOrDefault() != "fabriq_shared" {
		t.Fatalf("schema mode default wrong: %v", c.Catalog.sharedSchemaOrDefault())
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("valid schema-mode config rejected: %v", err)
	}
}
