package fabriqtest_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func tctx(t *testing.T, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestFakeCacheSetGet(t *testing.T) {
	c := fabriqtest.NewFakeCache()
	ks := cache.Keyspace{Name: "k", Version: 1, Partition: cache.Tenant,
		Policy: cache.Policy{Mode: cache.EventEvict}}
	ctx := tctx(t, "acme")

	if _, ok, err := c.Get(ctx, ks, "x"); err != nil || ok {
		t.Fatalf("expected miss, ok=%v err=%v", ok, err)
	}
	if err := c.Set(ctx, ks, "x", []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, ks, "x")
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("get: got=%q ok=%v err=%v", got, ok, err)
	}
}

func TestFakeCacheConformance(t *testing.T) {
	fabriqtest.RunCacheConformance(t, func(_ *testing.T) cache.Cache {
		return fabriqtest.NewFakeCache()
	})
}
