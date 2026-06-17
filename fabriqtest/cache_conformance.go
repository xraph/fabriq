package fabriqtest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/tenant"
)

// RunCacheConformance is the single behavioral contract for cache.Cache. It is
// run against FakeCache (unit) and the grove-kv adapter (integration) so the
// two can never drift. newCache returns a fresh, empty cache per subtest.
func RunCacheConformance(t *testing.T, newCache func(t *testing.T) cache.Cache) {
	t.Helper()

	mkTenant := func(t *testing.T, id string) context.Context {
		t.Helper()
		ctx, err := tenant.WithTenant(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		return ctx
	}
	mkScope := func(t *testing.T, id, scope string) context.Context {
		t.Helper()
		ctx, err := tenant.WithScope(mkTenant(t, id), scope)
		if err != nil {
			t.Fatal(err)
		}
		return ctx
	}

	t.Run("set/get/miss", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c1", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		ctx := mkTenant(t, "acme")
		if _, ok, err := c.Get(ctx, ks, "k"); err != nil || ok {
			t.Fatalf("miss expected: ok=%v err=%v", ok, err)
		}
		if err := c.Set(ctx, ks, "k", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		v, ok, err := c.Get(ctx, ks, "k")
		if err != nil || !ok || string(v) != "v1" {
			t.Fatalf("hit expected: v=%q ok=%v err=%v", v, ok, err)
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c2", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		if err := c.Set(mkTenant(t, "acme"), ks, "k", []byte("acme")); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(mkTenant(t, "globex"), ks, "k"); ok {
			t.Fatal("tenant globex must not see acme's entry")
		}
		v, ok, _ := c.Get(mkTenant(t, "acme"), ks, "k")
		if !ok || string(v) != "acme" {
			t.Fatalf("acme must see its own entry: v=%q ok=%v", v, ok)
		}
	})

	t.Run("scope isolation", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c3", Version: 1, Partition: cache.TenantScope,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		if err := c.Set(mkScope(t, "acme", "A"), ks, "k", []byte("A")); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(mkScope(t, "acme", "B"), ks, "k"); ok {
			t.Fatal("scope B must not see scope A's entry")
		}
		v, ok, _ := c.Get(mkScope(t, "acme", "A"), ks, "k")
		if !ok || string(v) != "A" {
			t.Fatalf("scope A must see its own entry: v=%q ok=%v", v, ok)
		}
	})

	t.Run("global is cross-tenant", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c4", Version: 1, Partition: cache.Global,
			Policy: cache.Policy{Mode: cache.TTLOnly, TTL: time.Hour}}
		if err := c.Set(mkTenant(t, "acme"), ks, "k", []byte("shared")); err != nil {
			t.Fatal(err)
		}
		// A different tenant (and even no tenant) sees the same global entry.
		v, ok, _ := c.Get(mkTenant(t, "globex"), ks, "k")
		if !ok || string(v) != "shared" {
			t.Fatalf("global entry must be visible cross-tenant: v=%q ok=%v", v, ok)
		}
		v2, ok2, _ := c.Get(context.Background(), ks, "k")
		if !ok2 || string(v2) != "shared" {
			t.Fatalf("global entry must be visible without tenant: v=%q ok=%v", v2, ok2)
		}
	})

	t.Run("invalidate key", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c5", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		ctx := mkTenant(t, "acme")
		_ = c.Set(ctx, ks, "k", []byte("v"))
		if err := c.Invalidate(ctx, ks, "k"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(ctx, ks, "k"); ok {
			t.Fatal("invalidated key must miss")
		}
	})

	t.Run("invalidate keyspace orphans all entries", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c6", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.Versioned}}
		ctx := mkTenant(t, "acme")
		_ = c.Set(ctx, ks, "a", []byte("1"))
		_ = c.Set(ctx, ks, "b", []byte("2"))
		if err := c.InvalidateKeyspace(ctx, ks); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(ctx, ks, "a"); ok {
			t.Fatal("a must miss after keyspace invalidation")
		}
		if _, ok, _ := c.Get(ctx, ks, "b"); ok {
			t.Fatal("b must miss after keyspace invalidation")
		}
		// A different keyspace in the same tenant is untouched.
		other := cache.Keyspace{Name: "c6b", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.Versioned}}
		_ = c.Set(ctx, other, "a", []byte("keep"))
		_ = c.InvalidateKeyspace(ctx, ks)
		if _, ok, _ := c.Get(ctx, other, "a"); !ok {
			t.Fatal("unrelated keyspace must survive")
		}
	})

	t.Run("getorload single-flights concurrent misses", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c7", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		ctx := mkTenant(t, "acme")

		const n = 16
		var loads atomic.Int64
		var start sync.WaitGroup
		start.Add(n)
		var done sync.WaitGroup
		done.Add(n)
		gate := make(chan struct{})

		for i := 0; i < n; i++ {
			go func() {
				defer done.Done()
				start.Done()
				<-gate // release all goroutines together
				_, _ = c.GetOrLoad(ctx, ks, "hot", func(context.Context) ([]byte, error) {
					loads.Add(1)
					time.Sleep(30 * time.Millisecond) // hold the flight open
					return []byte("v"), nil
				})
			}()
		}
		start.Wait()
		close(gate)
		done.Wait()

		if got := loads.Load(); got != 1 {
			t.Fatalf("loader ran %d times, want 1 (stampede protection)", got)
		}
		v, ok, _ := c.Get(ctx, ks, "hot")
		if !ok || string(v) != "v" {
			t.Fatalf("value should be cached after load: v=%q ok=%v", v, ok)
		}
	})

	t.Run("getorload caches and reuses", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c8", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.EventEvict}}
		ctx := mkTenant(t, "acme")
		var loads atomic.Int64
		load := func(context.Context) ([]byte, error) { loads.Add(1); return []byte("v"), nil }
		_, _ = c.GetOrLoad(ctx, ks, "k", load)
		_, _ = c.GetOrLoad(ctx, ks, "k", load)
		if loads.Load() != 1 {
			t.Fatalf("loader ran %d times, want 1", loads.Load())
		}
	})

	t.Run("ttl set then immediate get hits", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "c9", Version: 1, Partition: cache.Tenant,
			Policy: cache.Policy{Mode: cache.TTLOnly, TTL: time.Hour}}
		ctx := mkTenant(t, "acme")
		_ = c.Set(ctx, ks, "k", []byte("v"))
		if _, ok, _ := c.Get(ctx, ks, "k"); !ok {
			t.Fatal("entry with a long TTL must hit immediately")
		}
	})

	t.Run("invalidate entity busts entity-keyed keyspaces across partitions", func(t *testing.T) {
		c := newCache(t)
		// Two keyspaces over the SAME entity "asset": a query keyspace (Tenant)
		// and an entity-by-id keyspace (TenantScope). Both declare Entity.
		queryKS := cache.Keyspace{Name: "asset.q", Version: 1, Entity: "asset",
			Partition: cache.Tenant, Policy: cache.Policy{Mode: cache.Versioned}}
		entityKS := cache.Keyspace{Name: "asset.byid", Version: 1, Entity: "asset",
			Partition: cache.TenantScope, Policy: cache.Policy{Mode: cache.Versioned}}
		// An unrelated entity's keyspace must survive.
		otherKS := cache.Keyspace{Name: "site.q", Version: 1, Entity: "site",
			Partition: cache.Tenant, Policy: cache.Policy{Mode: cache.Versioned}}

		tctx := mkTenant(t, "acme")
		sctx := mkScope(t, "acme", "A")
		sctxB := mkScope(t, "acme", "B")
		_ = c.Set(tctx, queryKS, "fp1", []byte("q"))
		_ = c.Set(sctx, entityKS, "id1", []byte("row"))
		_ = c.Set(sctxB, entityKS, "id2", []byte("rowB"))
		_ = c.Set(tctx, otherKS, "fp2", []byte("other"))

		// A write to "asset" in tenant acme, scope A.
		if err := c.InvalidateEntity(sctx, "asset"); err != nil {
			t.Fatal(err)
		}

		if _, ok, _ := c.Get(tctx, queryKS, "fp1"); ok {
			t.Fatal("asset query keyspace (Tenant) must be busted")
		}
		if _, ok, _ := c.Get(sctx, entityKS, "id1"); ok {
			t.Fatal("asset entity keyspace (TenantScope) must be busted")
		}
		if _, ok, _ := c.Get(tctx, otherKS, "fp2"); !ok {
			t.Fatal("unrelated entity 'site' keyspace must survive")
		}
		if _, ok, _ := c.Get(sctxB, entityKS, "id2"); !ok {
			t.Fatal("a sibling scope (B) entry must survive invalidation scoped to A")
		}
	})

	t.Run("invalidate entity is tenant-isolated", func(t *testing.T) {
		c := newCache(t)
		ks := cache.Keyspace{Name: "asset.q", Version: 1, Entity: "asset",
			Partition: cache.Tenant, Policy: cache.Policy{Mode: cache.Versioned}}
		_ = c.Set(mkTenant(t, "acme"), ks, "fp", []byte("a"))
		_ = c.Set(mkTenant(t, "globex"), ks, "fp", []byte("g"))

		// Invalidate asset in acme only.
		if err := c.InvalidateEntity(mkTenant(t, "acme"), "asset"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(mkTenant(t, "acme"), ks, "fp"); ok {
			t.Fatal("acme asset cache must be busted")
		}
		if _, ok, _ := c.Get(mkTenant(t, "globex"), ks, "fp"); !ok {
			t.Fatal("globex asset cache must survive (tenant isolation)")
		}
	})

	t.Run("invalidate entity also busts the global tier", func(t *testing.T) {
		c := newCache(t)
		globalKS := cache.Keyspace{Name: "asset.global", Version: 1, Entity: "asset",
			Partition: cache.Global, Policy: cache.Policy{Mode: cache.Versioned}}
		_ = c.Set(mkTenant(t, "acme"), globalKS, "fp", []byte("shared"))
		if err := c.InvalidateEntity(mkTenant(t, "acme"), "asset"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := c.Get(context.Background(), globalKS, "fp"); ok {
			t.Fatal("a write to asset must bust the Global asset keyspace")
		}
	})
}
