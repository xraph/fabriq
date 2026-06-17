//go:build integration

package cache_test

import (
	"context"
	"testing"
	"time"

	fcache "github.com/xraph/fabriq/adapters/cache"
	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestRedisCacheConformance(t *testing.T) {
	addr := fabriqtest.StartRedis(t)
	fabriqtest.RunCacheConformance(t, func(t *testing.T) cache.Cache {
		a, err := fcache.Open(context.Background(), fcache.Config{Addr: addr})
		if err != nil {
			t.Fatalf("open cache adapter: %v", err)
		}
		t.Cleanup(func() { _ = a.Close() })
		return a
	})
}

func TestRedisCacheTTLExpires(t *testing.T) {
	addr := fabriqtest.StartRedis(t)
	a, err := fcache.Open(context.Background(), fcache.Config{Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	ks := cache.Keyspace{Name: "ttl", Version: 1, Partition: cache.Tenant,
		Policy: cache.Policy{Mode: cache.TTLOnly, TTL: time.Second}}
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Set(ctx, ks, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := a.Get(ctx, ks, "k"); !ok {
		t.Fatal("expected immediate hit")
	}
	time.Sleep(1500 * time.Millisecond)
	if _, ok, _ := a.Get(ctx, ks, "k"); ok {
		t.Fatal("expected expiry after TTL")
	}
}
