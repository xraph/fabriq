package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
)

// TestProbe_ForgeConfig is a throwaway probe to learn how forge/confy load
// config.yaml + env for fabriq's keys. Delete after.
func TestProbe_ForgeConfig(t *testing.T) {
	dir := t.TempDir()
	yaml := `
postgres:
  dsn: postgres://file@pg:5432/fabriq?sslmode=require
  pool_size: 24
redis:
  addr: redis-file:6379
falkordb:
  addr: falkor-file:6379
elasticsearch:
  addrs:
    - https://es-file:9200
projections:
  graph: true
  search: true
subscriptions:
  conflation_window: 200ms
  stream_max_len: 1000
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	// Env overrides (the chart's contract).
	t.Setenv("FABRIQ_POSTGRES_DSN", "postgres://env@pg:5432/fabriq?sslmode=require")
	t.Setenv("FABRIQ_ELASTICSEARCH_ADDRS", "https://es-env1:9200,https://es-env2:9200")

	app := forge.NewApp(forge.AppConfig{
		Name:              "fabriq",
		Version:           "0.0.0",
		ConfigSearchPaths: []string{dir},
	})

	cm := app.Config()
	fmt.Printf("PROBE cm==nil? %v\n", cm == nil)
	if cm == nil {
		t.Fatal("app.Config() nil right after NewApp")
	}
	for _, k := range []string{"postgres", "postgres.dsn", "postgres.pool_size", "redis", "elasticsearch", "elasticsearch.addrs", "projections.graph", "subscriptions.conflation_window"} {
		fmt.Printf("PROBE IsSet(%q)=%v  Get=%v\n", k, cm.IsSet(k), cm.Get(k))
	}

	var cfg fabriq.Config
	for _, b := range []struct {
		key    string
		target any
	}{
		{"postgres", &cfg.Postgres},
		{"redis", &cfg.Redis},
		{"falkordb", &cfg.FalkorDB},
		{"elasticsearch", &cfg.Elasticsearch},
		{"projections", &cfg.Projections},
		{"subscriptions", &cfg.Subscriptions},
	} {
		err := cm.Bind(b.key, b.target)
		fmt.Printf("PROBE Bind(%q) err=%v\n", b.key, err)
	}
	fmt.Printf("PROBE RESULT cfg=%+v\n", cfg)
	fmt.Printf("PROBE postgres.dsn=%q (want env)\n", cfg.Postgres.DSN)
	fmt.Printf("PROBE es.addrs=%v (want env split)\n", cfg.Elasticsearch.Addrs)
	fmt.Printf("PROBE projections=%+v\n", cfg.Projections)
	fmt.Printf("PROBE subs=%+v\n", cfg.Subscriptions)
}
