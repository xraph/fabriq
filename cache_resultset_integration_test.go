//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

func TestResultSetCacheListWarmAndInvalidate(t *testing.T) {
	reg, appDSN, redisAddr := setupCacheE2E(t)
	if ent, ok := reg.Get("asset"); ok {
		ent.Spec.Cache = &registry.CacheSpec{TTL: time.Minute}
	}
	f, stores, err := fabriq.Open(context.Background(), reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump", SiteID: "s1"}}); err != nil {
		t.Fatal(err)
	}
	repo, err := fabriq.For[domain.Asset](f)
	if err != nil {
		t.Fatal(err)
	}

	// Warm the List result set.
	first, err := repo.List(ctx, query.ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("expected 1 asset, got %d", len(first))
	}

	// A committed create bumps the generation → cached List must reflect it.
	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Valve", SiteID: "s1"}}); err != nil {
		t.Fatal(err)
	}
	second, err := repo.List(ctx, query.ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 {
		t.Fatalf("after a committed create, cached List must re-resolve: got %d (want 2)", len(second))
	}
}
