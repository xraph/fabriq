package cachequery_test

import (
	"testing"
	"time"

	"github.com/xraph/fabriq/cachequery"
	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
)

func TestEntityQueryKeyspace(t *testing.T) {
	ks := cachequery.EntityQueryKeyspace(entWithCache(t, "asset", time.Minute, true))
	if ks.Name != "asset:q" || ks.Entity != "asset" {
		t.Fatalf("name/entity wrong: %+v", ks)
	}
	if ks.Partition != cache.TenantScope {
		t.Fatalf("scoped entity must use TenantScope, got %v", ks.Partition)
	}
	if ks.Policy.Mode != cache.Versioned || ks.Policy.TTL != time.Minute {
		t.Fatalf("policy wrong: %+v", ks.Policy)
	}
	ks2 := cachequery.EntityQueryKeyspace(entWithCache(t, "site", time.Hour, false))
	if ks2.Partition != cache.Tenant {
		t.Fatalf("unscoped entity must use Tenant, got %v", ks2.Partition)
	}
}

func entWithCache(t *testing.T, name string, ttl time.Duration, scoped bool) *registry.Entity {
	t.Helper()
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name: name, Kind: registry.KindAggregate, Model: row{},
		Cache: &registry.CacheSpec{TTL: ttl, Scoped: scoped},
	}); err != nil {
		t.Fatal(err)
	}
	ent, _ := r.Get(name)
	return ent
}
