package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/forge"
)

// appConfigForTest mirrors the config-loading fields setup() puts on the
// forge app, pointed at a temp search dir.
func appConfigForTest(dir string) forge.AppConfig {
	return forge.AppConfig{
		Name:                      "fabriq-worker",
		Version:                   "0.0.0",
		EnableConfigAutoDiscovery: true,
		EnableEnvConfig:           true,
		EnvOverridesFile:          true,
		EnvPrefix:                 "FABRIQ_",
		ConfigSearchPaths:         []string{dir},
	}
}

// TestLoadFabriqConfig_FileAndEnv proves config.yaml is discovered, FABRIQ_*
// env overrides the file, and every section binds (including the []string
// elasticsearch.addrs and the duration subscriptions.conflation_window).
func TestLoadFabriqConfig_FileAndEnv(t *testing.T) {
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
  stream_max_len: 2048
  subscribe_buffer: 128
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FABRIQ_POSTGRES_DSN", "postgres://env@pg:5432/fabriq?sslmode=require")
	t.Setenv("FABRIQ_ELASTICSEARCH_ADDRS", "https://es-env1:9200,https://es-env2:9200")

	app := forge.NewApp(appConfigForTest(dir))
	cfg := loadFabriqConfig(app.Config())

	if got, want := cfg.Postgres.DSN, "postgres://env@pg:5432/fabriq?sslmode=require"; got != want {
		t.Errorf("postgres.dsn = %q, want env override %q", got, want)
	}
	if cfg.Postgres.PoolSize != 24 {
		t.Errorf("postgres.pool_size = %d, want 24 (from file)", cfg.Postgres.PoolSize)
	}
	if cfg.Redis.Addr != "redis-file:6379" {
		t.Errorf("redis.addr = %q, want file value", cfg.Redis.Addr)
	}
	if cfg.FalkorDB.Addr != "falkor-file:6379" {
		t.Errorf("falkordb.addr = %q, want file value", cfg.FalkorDB.Addr)
	}
	if len(cfg.Elasticsearch.Addrs) != 2 || cfg.Elasticsearch.Addrs[0] != "https://es-env1:9200" {
		t.Errorf("elasticsearch.addrs = %v, want 2 env-split entries", cfg.Elasticsearch.Addrs)
	}
	if !cfg.Projections.Graph || !cfg.Projections.Search {
		t.Errorf("projections = %+v, want both true", cfg.Projections)
	}
	if cfg.Subscriptions.ConflationWindow != 200*time.Millisecond {
		t.Errorf("subscriptions.conflation_window = %v, want 200ms", cfg.Subscriptions.ConflationWindow)
	}
	if cfg.Subscriptions.StreamMaxLen != 2048 || cfg.Subscriptions.SubscribeBuffer != 128 {
		t.Errorf("subscriptions = %+v, want maxlen 2048 / buffer 128", cfg.Subscriptions)
	}
}

// TestLoadFabriqConfig_EnvOnly is the Kubernetes/Helm path: no config.yaml
// is mounted, connections come purely from FABRIQ_* env. This must work or
// the deployment breaks.
func TestLoadFabriqConfig_EnvOnly(t *testing.T) {
	dir := t.TempDir() // empty — no config.yaml
	t.Setenv("FABRIQ_POSTGRES_DSN", "postgres://only@pg/db")
	t.Setenv("FABRIQ_REDIS_ADDR", "r:6379")
	t.Setenv("FABRIQ_FALKORDB_ADDR", "falkor:6379")

	app := forge.NewApp(appConfigForTest(dir))
	cfg := loadFabriqConfig(app.Config())

	if cfg.Postgres.DSN != "postgres://only@pg/db" {
		t.Errorf("postgres.dsn = %q, want env-only value", cfg.Postgres.DSN)
	}
	if cfg.Redis.Addr != "r:6379" {
		t.Errorf("redis.addr = %q, want env-only value", cfg.Redis.Addr)
	}
	if cfg.FalkorDB.Addr != "falkor:6379" {
		t.Errorf("falkordb.addr = %q, want env-only value", cfg.FalkorDB.Addr)
	}
}

func TestLoadFabriqConfig_NilManager(t *testing.T) {
	if cfg := loadFabriqConfig(nil); cfg.Postgres.DSN != "" || cfg.Redis.Addr != "" {
		t.Errorf("nil config manager should yield a zero Config, got %+v", cfg)
	}
}
