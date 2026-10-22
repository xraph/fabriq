package forgeext_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xraph/confy"
	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/forgeext"
)

func TestOptions_ApplyOverDefaults(t *testing.T) {
	var c forgeext.Config
	for _, o := range []forgeext.Option{
		forgeext.WithConfig(fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: "pg"}}),
		forgeext.WithWorker(true),
		forgeext.WithReconcileInterval(2 * time.Minute),
	} {
		o(&c)
	}
	if c.Fabriq.Postgres.DSN != "pg" || !c.RunWorker || c.ReconcileInterval != 2*time.Minute {
		t.Fatalf("options not applied: %+v", c)
	}
}

func TestLoadConfig_NilManager(t *testing.T) {
	if got := forgeext.LoadConfig(nil, ""); got.Postgres.DSN != "" {
		t.Fatalf("nil manager should yield zero config, got %+v", got)
	}
}

// TestLoadConfig_TopLevel verifies LoadConfig reads top-level keys (prefix="")
// from an in-memory confy manager — the same contract as cmd/fabriq serve.
func TestLoadConfig_TopLevel(t *testing.T) {
	cm := confy.NewTestConfyImplWithData(map[string]any{
		"postgres": map[string]any{
			"dsn": "postgres://test/db",
		},
	})
	got := forgeext.LoadConfig(cm, "")
	if got.Postgres.DSN != "postgres://test/db" {
		t.Fatalf("LoadConfig top-level: expected DSN %q, got %q", "postgres://test/db", got.Postgres.DSN)
	}
}

// TestLoadConfig_PrefixedKey verifies LoadConfig reads under a dotted prefix —
// the extensions.fabriq.* convention for host-app embedding.
func TestLoadConfig_PrefixedKey(t *testing.T) {
	cm := confy.NewTestConfyImplWithData(map[string]any{
		"extensions": map[string]any{
			"fabriq": map[string]any{
				"postgres": map[string]any{
					"dsn": "postgres://ext/db",
				},
			},
		},
	})
	got := forgeext.LoadConfig(cm, "extensions.fabriq.")
	if got.Postgres.DSN != "postgres://ext/db" {
		t.Fatalf("LoadConfig prefixed: expected DSN %q, got %q", "postgres://ext/db", got.Postgres.DSN)
	}
}

// TestLoadConfig_ShardPins verifies the shardPins map binds from the config
// manager alongside the shards list (config.yaml-only, like shards itself —
// the FABRIQ_* env overlay cannot express a map).
func TestLoadConfig_ShardPins(t *testing.T) {
	cm := confy.NewTestConfyImplWithData(map[string]any{
		// []any of map[string]any is the shape YAML parsing produces.
		"shards": []any{
			map[string]any{"id": "shard-a", "dsn": "postgres://pg-a/fabriq"},
			map[string]any{"id": "shard-b", "dsn": "postgres://pg-b/fabriq"},
		},
		"shardPins": map[string]any{
			"acme": "shard-b",
		},
	})
	got := forgeext.LoadConfig(cm, "")
	if len(got.Shards) != 2 {
		t.Fatalf("shards did not bind: %+v", got.Shards)
	}
	if got.ShardPins["acme"] != "shard-b" {
		t.Fatalf("shardPins did not bind: %+v", got.ShardPins)
	}
}

func TestOptions_Distill(t *testing.T) {
	var cfg forgeext.Config
	for _, o := range []forgeext.Option{
		forgeext.WithSummarizer(nil), forgeext.WithGuard(nil),
		forgeext.WithDistillFailOpenGuard(true),
		forgeext.WithDistillRecipeVersion("v2"),
		forgeext.WithDistillDebounce(2 * time.Second),
	} {
		o(&cfg)
	}
	if !cfg.DistillFailOpenGuard || cfg.DistillRecipeVersion != "v2" || cfg.DistillDebounce != 2*time.Second {
		t.Fatalf("distill options not applied: %+v", cfg)
	}
}

// TestLoadConfig_DynamicSection verifies the dynamic block binds both fields.
// It goes through a real forge app and a config.yaml, the way cmd/fabriq does,
// because confy's in-memory test manager does not decode durations.
func TestLoadConfig_DynamicSection(t *testing.T) {
	dir := t.TempDir()
	yaml := "dynamic:\n  persist: true\n  syncInterval: 30s\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	app := forge.NewApp(forge.AppConfig{
		Name:                      "fabriq",
		Version:                   "0.0.0",
		EnableConfigAutoDiscovery: true,
		EnableEnvConfig:           true,
		EnvOverridesFile:          true,
		EnvPrefix:                 "FABRIQ_",
		ConfigSearchPaths:         []string{dir},
	})
	got := forgeext.LoadConfig(app.Config(), "")
	if !got.Dynamic.Persist {
		t.Errorf("dynamic.persist = false, want true")
	}
	if got.Dynamic.SyncInterval != 30*time.Second {
		t.Errorf("dynamic.syncInterval = %v, want 30s", got.Dynamic.SyncInterval)
	}
}

// newEnvConfigApp builds a forge app the way cmd/fabriq does (config.yaml
// discovery in dir plus the FABRIQ_ env overlay) and returns its config
// manager. yaml is written to dir/config.yaml unless it is empty.
func newEnvConfigApp(t *testing.T, yaml string) forge.ConfigManager {
	t.Helper()
	dir := t.TempDir()
	if yaml != "" {
		if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	app := forge.NewApp(forge.AppConfig{
		Name:                      "fabriq",
		Version:                   "0.0.0",
		EnableConfigAutoDiscovery: true,
		EnableEnvConfig:           true,
		EnvOverridesFile:          true,
		EnvPrefix:                 "FABRIQ_",
		ConfigSearchPaths:         []string{dir},
	})
	return app.Config()
}

// TestLoadConfig_StoreCredentialsFromEnv pins the hosted contract: with no
// config file, the store addresses and all six credentials come from env.
func TestLoadConfig_StoreCredentialsFromEnv(t *testing.T) {
	env := map[string]string{
		"FABRIQ_REDIS_ADDR":             "redis:6379",
		"FABRIQ_REDIS_USERNAME":         "ru",
		"FABRIQ_REDIS_PASSWORD":         "rp",
		"FABRIQ_FALKORDB_ADDR":          "falkor:6379",
		"FABRIQ_FALKORDB_USERNAME":      "fu",
		"FABRIQ_FALKORDB_PASSWORD":      "fp",
		"FABRIQ_ELASTICSEARCH_ADDRS":    "http://es:9200",
		"FABRIQ_ELASTICSEARCH_USERNAME": "eu",
		"FABRIQ_ELASTICSEARCH_PASSWORD": "ep",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	got := forgeext.LoadConfig(newEnvConfigApp(t, ""), "")

	want := fabriq.Config{
		Redis:         fabriq.RedisConfig{Addr: "redis:6379", Username: "ru", Password: "rp"},
		FalkorDB:      fabriq.FalkorDBConfig{Addr: "falkor:6379", Username: "fu", Password: "fp"},
		Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{"http://es:9200"}, Username: "eu", Password: "ep"},
	}
	if got.Redis != want.Redis {
		t.Errorf("redis = %+v, want %+v", got.Redis, want.Redis)
	}
	if got.FalkorDB != want.FalkorDB {
		t.Errorf("falkordb = %+v, want %+v", got.FalkorDB, want.FalkorDB)
	}
	if len(got.Elasticsearch.Addrs) != 1 || got.Elasticsearch.Addrs[0] != "http://es:9200" ||
		got.Elasticsearch.Username != "eu" || got.Elasticsearch.Password != "ep" {
		t.Errorf("elasticsearch = %+v, want %+v", got.Elasticsearch, want.Elasticsearch)
	}
}

// TestLoadConfig_StoreCredentialsEnvOverridesFile: a set env var wins over
// config.yaml, and a file-only credential survives when its env var is unset.
func TestLoadConfig_StoreCredentialsEnvOverridesFile(t *testing.T) {
	yaml := "redis:\n  addr: redis:6379\n  password: file\nfalkordb:\n  addr: falkor:6379\n  password: falkor-file\n"
	t.Setenv("FABRIQ_REDIS_PASSWORD", "env")
	got := forgeext.LoadConfig(newEnvConfigApp(t, yaml), "")
	if got.Redis.Password != "env" {
		t.Errorf("redis.password = %q, want env", got.Redis.Password)
	}
	if got.FalkorDB.Password != "falkor-file" {
		t.Errorf("falkordb.password = %q, want falkor-file", got.FalkorDB.Password)
	}
}

// TestLoadConfig_ExtensionPrefixIgnoresBareEnv: a host app reading
// extensions.fabriq.* never picks up the bare FABRIQ_* credentials, which
// belong to the cmd/fabriq contract.
func TestLoadConfig_ExtensionPrefixIgnoresBareEnv(t *testing.T) {
	yaml := "extensions:\n  fabriq:\n    redis:\n      addr: redis:6379\n"
	t.Setenv("FABRIQ_REDIS_PASSWORD", "env")
	got := forgeext.LoadConfig(newEnvConfigApp(t, yaml), "extensions.fabriq.")
	if got.Redis.Addr != "redis:6379" {
		t.Fatalf("redis.addr = %q, want redis:6379 from the file", got.Redis.Addr)
	}
	if got.Redis.Password != "" {
		t.Errorf("redis.password = %q, want empty under the extension prefix", got.Redis.Password)
	}
}

// TestLoadConfig_StoreCredentialsOverlay proves the explicit overlay on its
// own: this config manager has no env source at all, so only LoadConfig's
// overlay can carry the credentials in. An unset variable keeps the
// configured value.
func TestLoadConfig_StoreCredentialsOverlay(t *testing.T) {
	cm := confy.NewTestConfyImplWithData(map[string]any{
		"redis":    map[string]any{"addr": "redis:6379", "password": "file"},
		"falkordb": map[string]any{"addr": "falkor:6379", "password": "falkor-file"},
	})
	t.Setenv("FABRIQ_REDIS_USERNAME", "ru")
	t.Setenv("FABRIQ_REDIS_PASSWORD", "rp")
	t.Setenv("FABRIQ_FALKORDB_USERNAME", "fu")
	t.Setenv("FABRIQ_ELASTICSEARCH_USERNAME", "eu")
	t.Setenv("FABRIQ_ELASTICSEARCH_PASSWORD", "ep")
	got := forgeext.LoadConfig(cm, "")
	if got.Redis.Username != "ru" || got.Redis.Password != "rp" {
		t.Errorf("redis = %+v, want ru/rp from env", got.Redis)
	}
	if got.FalkorDB.Username != "fu" || got.FalkorDB.Password != "falkor-file" {
		t.Errorf("falkordb = %+v, want fu from env and falkor-file from config", got.FalkorDB)
	}
	if got.Elasticsearch.Username != "eu" || got.Elasticsearch.Password != "ep" {
		t.Errorf("elasticsearch = %+v, want eu/ep from env", got.Elasticsearch)
	}

	if prefixed := forgeext.LoadConfig(cm, "extensions.fabriq."); prefixed.Redis.Password != "" {
		t.Errorf("prefixed redis.password = %q, want empty", prefixed.Redis.Password)
	}
}
