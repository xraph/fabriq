package forgeext_test

import (
	"testing"
	"time"

	"github.com/xraph/confy"

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
