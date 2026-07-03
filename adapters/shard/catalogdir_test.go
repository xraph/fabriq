package shard_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/fabriqtest"
)

// countingCatalog wraps a catalog and counts Get calls (cache assertions).
type countingCatalog struct {
	catalog.Catalog
	gets int
}

func (c *countingCatalog) Get(ctx context.Context, tenantID string) (catalog.Entry, error) {
	c.gets++
	return c.Catalog.Get(ctx, tenantID)
}

func codeOf(err error) fabriqerr.Code {
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return fe.Code
	}
	return ""
}

func seedTenant(t *testing.T, cat catalog.Catalog, tenant string, state catalog.State) catalog.Entry {
	t.Helper()
	e, err := cat.Put(context.Background(), catalog.Entry{
		TenantID: tenant, ClusterID: "c1", Database: "fabriq_" + tenant, State: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCatalogDirectory_ResolvesActive(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	seedTenant(t, cat, "acme", catalog.StateActive)
	dir := shard.CatalogDirectory(cat, time.Minute)

	id, err := dir.Shard(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if id != "c1/fabriq_acme" {
		t.Fatalf("shard id = %q, want c1/fabriq_acme", id)
	}
}

func TestCatalogDirectory_UnknownTenantIsNotFound(t *testing.T) {
	dir := shard.CatalogDirectory(fabriqtest.NewFakeCatalog(), time.Minute)
	_, err := dir.Shard(context.Background(), "ghost")
	if codeOf(err) != fabriqerr.CodeNotFound {
		t.Fatalf("err = %v, want CodeNotFound", err)
	}
}

func TestCatalogDirectory_NonActiveStatesAreUnavailable(t *testing.T) {
	for _, state := range []catalog.State{
		catalog.StatePending, catalog.StateCreating, catalog.StateMigrating,
		catalog.StateSuspended, catalog.StateFailed,
	} {
		cat := fabriqtest.NewFakeCatalog()
		seedTenant(t, cat, "acme", state)
		dir := shard.CatalogDirectory(cat, time.Minute)
		_, err := dir.Shard(context.Background(), "acme")
		if codeOf(err) != fabriqerr.CodeUnavailable {
			t.Fatalf("state %s: err = %v, want CodeUnavailable", state, err)
		}
	}
}

func TestCatalogDirectory_CachesWithinTTL(t *testing.T) {
	cat := &countingCatalog{Catalog: fabriqtest.NewFakeCatalog()}
	seedTenant(t, cat.Catalog, "acme", catalog.StateActive)
	now := time.Unix(1000, 0)
	dir := shard.CatalogDirectoryWithClock(cat, time.Minute, func() time.Time { return now })

	for i := 0; i < 5; i++ {
		if _, err := dir.Shard(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
	}
	if cat.gets != 1 {
		t.Fatalf("catalog gets = %d, want 1 (cached)", cat.gets)
	}
	// TTL expiry re-resolves.
	now = now.Add(2 * time.Minute)
	if _, err := dir.Shard(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if cat.gets != 2 {
		t.Fatalf("catalog gets = %d, want 2 after expiry", cat.gets)
	}
}

func TestCatalogDirectory_NegativeCacheExpires(t *testing.T) {
	inner := fabriqtest.NewFakeCatalog()
	cat := &countingCatalog{Catalog: inner}
	now := time.Unix(1000, 0)
	dir := shard.CatalogDirectoryWithClock(cat, time.Minute, func() time.Time { return now })
	ctx := context.Background()

	// Repeated misses hit the cache, not the catalog (miss-storm shield).
	for i := 0; i < 5; i++ {
		if _, err := dir.Shard(ctx, "late"); codeOf(err) != fabriqerr.CodeNotFound {
			t.Fatalf("err = %v, want CodeNotFound", err)
		}
	}
	if cat.gets != 1 {
		t.Fatalf("catalog gets = %d, want 1 (negative-cached)", cat.gets)
	}

	// The tenant is provisioned after the miss; it becomes routable once
	// the negative entry expires.
	seedTenant(t, inner, "late", catalog.StateActive)
	now = now.Add(2 * time.Minute)
	id, err := dir.Shard(ctx, "late")
	if err != nil {
		t.Fatal(err)
	}
	if id != "c1/fabriq_late" {
		t.Fatalf("shard id = %q", id)
	}
}

func TestCatalogDirectory_SuspensionTakesEffectAfterTTL(t *testing.T) {
	inner := fabriqtest.NewFakeCatalog()
	e := seedTenant(t, inner, "acme", catalog.StateActive)
	now := time.Unix(1000, 0)
	dir := shard.CatalogDirectoryWithClock(inner, time.Minute, func() time.Time { return now })
	ctx := context.Background()

	if _, err := dir.Shard(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	e.State = catalog.StateSuspended
	if _, err := inner.Put(ctx, e); err != nil {
		t.Fatal(err)
	}
	// Cached route keeps serving inside the TTL (documented freshness bound)…
	if _, err := dir.Shard(ctx, "acme"); err != nil {
		t.Fatalf("within TTL: %v", err)
	}
	// …and routes off once it expires.
	now = now.Add(2 * time.Minute)
	if _, err := dir.Shard(ctx, "acme"); codeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("after TTL: want CodeUnavailable")
	}
}

// BenchmarkCatalogDirectory_CachedResolve is the request hot path. Spec
// target: < 100 ns/op, 0 allocs.
func BenchmarkCatalogDirectory_CachedResolve(b *testing.B) {
	cat := fabriqtest.NewFakeCatalog()
	if _, err := cat.Put(context.Background(), catalog.Entry{
		TenantID: "acme", ClusterID: "c1", Database: "db", State: catalog.StateActive,
	}); err != nil {
		b.Fatal(err)
	}
	dir := shard.CatalogDirectory(cat, time.Minute)
	ctx := context.Background()
	if _, err := dir.Shard(ctx, "acme"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := dir.Shard(ctx, "acme"); err != nil {
			b.Fatal(err)
		}
	}
}
