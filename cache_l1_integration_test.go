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

// TestL1_ReadYourWritesOnSameNode proves that, with L1 enabled, the writing
// node's in-process cache is synchronously evicted by the post-commit
// invalidator so that a subsequent Get reflects the committed update
// (read-your-writes guarantee on the same node).
func TestL1_ReadYourWritesOnSameNode(t *testing.T) {
	reg, appDSN, redisAddr := setupCacheE2E(t)
	if ent, ok := reg.Get("asset"); ok {
		ent.Spec.Cache = &registry.CacheSpec{TTL: time.Minute}
	} else {
		t.Fatal("asset entity not registered")
	}

	f, stores, err := fabriq.Open(context.Background(), reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
		Cache:    fabriq.CacheConfig{L1Enabled: true, L1Size: 1024, L1TTL: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Create the asset.
	r1, err := f.Exec(ctx, command.Command{
		Entity:  "asset",
		Op:      command.OpCreate,
		Payload: &domain.Asset{Name: "Pump", SiteID: "s1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Warm the L1 via a Get.
	var a1 domain.Asset
	if err := f.Relational().Get(ctx, "asset", r1.AggID, &a1); err != nil {
		t.Fatal(err) // warms L1
	}
	if a1.Name != "Pump" {
		t.Fatalf("initial Get: expected %q got %q", "Pump", a1.Name)
	}

	// Update on the same node — the invalidator must evict this node's L1.
	v := r1.Version
	if _, err := f.Exec(ctx, command.Command{
		Entity:          "asset",
		Op:              command.OpUpdate,
		AggID:           r1.AggID,
		ExpectedVersion: &v,
		Payload:         &domain.Asset{Name: "Pump v2", SiteID: "s1"},
	}); err != nil {
		t.Fatal(err)
	}

	// Post-update Get must return the new value — not the stale L1 entry.
	var a1b domain.Asset
	if err := f.Relational().Get(ctx, "asset", r1.AggID, &a1b); err != nil {
		t.Fatal(err)
	}
	if a1b.Name != "Pump v2" {
		t.Fatalf("L1 must be evicted on the writing node: got %q, want %q", a1b.Name, "Pump v2")
	}
}
