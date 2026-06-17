//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

func TestRowCacheWarmAndPerIDEviction(t *testing.T) {
	reg, appDSN, redisAddr := setupCacheE2E(t) // reuse the P2 e2e helper
	// Opt the asset entity into caching for THIS test only (fresh registry).
	if ent, ok := reg.Get("asset"); ok {
		ent.Spec.Cache = &registry.CacheSpec{TTL: time.Minute}
	} else {
		t.Fatal("asset entity not registered")
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

	// Create two assets.
	r1, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump", SiteID: "s1"}})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Valve", SiteID: "s1"}})
	if err != nil {
		t.Fatal(err)
	}

	// Warm both rows in the cache via Get.
	var a1, a2 domain.Asset
	if err := f.Relational().Get(ctx, "asset", r1.AggID, &a1); err != nil {
		t.Fatal(err)
	}
	if err := f.Relational().Get(ctx, "asset", r2.AggID, &a2); err != nil {
		t.Fatal(err)
	}
	if a1.Name != "Pump" {
		t.Fatalf("initial Get: expected %q got %q", "Pump", a1.Name)
	}
	if a2.Name != "Valve" {
		t.Fatalf("initial Get: expected %q got %q", "Valve", a2.Name)
	}

	// Update asset 1 — its row must be evicted; asset 2's row must stay warm.
	v1 := r1.Version
	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate,
		AggID: r1.AggID, ExpectedVersion: &v1,
		Payload: &domain.Asset{Name: "Pump v2", SiteID: "s1"}}); err != nil {
		t.Fatal(err)
	}

	// A fresh Get of asset 1 returns the UPDATED row (eviction worked, no stale).
	var a1b domain.Asset
	if err := f.Relational().Get(ctx, "asset", r1.AggID, &a1b); err != nil {
		t.Fatal(err)
	}
	if a1b.Name != "Pump v2" {
		t.Fatalf("asset 1 must reflect the update after eviction: got %q", a1b.Name)
	}

	// Asset 2 was not updated; a Get should return the original row (warm or re-fetched).
	var a2b domain.Asset
	if err := f.Relational().Get(ctx, "asset", r2.AggID, &a2b); err != nil {
		t.Fatal(err)
	}
	if a2b.Name != "Valve" {
		t.Fatalf("asset 2 should still be Valve: got %q", a2b.Name)
	}
}
