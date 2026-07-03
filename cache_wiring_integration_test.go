//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestOpenWiresCache(t *testing.T) {
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

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	_, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
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

	if stores.Cache == nil {
		t.Fatal("expected Stores.Cache to be wired when Redis is configured")
	}

	ks := cache.Keyspace{
		Name:      "wire",
		Version:   1,
		Partition: cache.Tenant,
		Policy:    cache.Policy{Mode: cache.EventEvict},
	}
	tenantCtx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	if err := stores.Cache.Set(tenantCtx, ks, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	val, ok, err := stores.Cache.Get(tenantCtx, ks, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("wired cache should round-trip")
	}
	if string(val) != "v" {
		t.Fatalf("expected value %q, got %q", "v", string(val))
	}
}
