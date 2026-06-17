//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// cacheE2EAsset is a minimal asset payload for the cache invalidation test.
type cacheE2EAsset struct {
	Name   string `grove:"name,notnull" json:"name"`
	SiteID string `grove:"site_id" json:"site_id"`
}

// setupCacheE2E boots Postgres+Redis containers, runs migrations, and
// provisions the app role. It mirrors the setup used in e2e_integration_test.go.
func setupCacheE2E(t *testing.T) (*registry.Registry, string, string) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	return reg, appDSN, redisAddr
}

// TestWriteInvalidatesEntityCache proves the post-commit hook is wired: a
// committed write to an entity must bust cached query entries declared for that
// entity (read-your-writes, no before-commit race).
func TestWriteInvalidatesEntityCache(t *testing.T) {
	reg, appDSN, redisAddr := setupCacheE2E(t)

	f, stores, err := fabriq.Open(context.Background(), reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
		Subscriptions: fabriq.SubscriptionsConfig{
			ConflationWindow: 30 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	ks := cache.Keyspace{
		Name:      "asset.q",
		Version:   1,
		Entity:    "asset",
		Partition: cache.Tenant,
		Policy:    cache.Policy{Mode: cache.Versioned},
	}

	// Pre-populate a cached query result over "asset".
	if err := stores.Cache.Set(ctx, ks, "all", []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := stores.Cache.Get(ctx, ks, "all"); !ok {
		t.Fatal("precondition: entry should be cached before the write")
	}

	// A committed write to an asset must bust the cached entry.
	if _, err := f.Exec(ctx, command.Command{
		Entity:  "asset",
		Op:      command.OpCreate,
		Payload: &domain.Asset{Name: "Pump", SiteID: ""},
	}); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := stores.Cache.Get(ctx, ks, "all"); ok {
		t.Fatal("cached asset query must be invalidated after a committed write")
	}
}
