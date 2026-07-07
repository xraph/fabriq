package shard

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// gateCat returns an ACTIVE entry whose version is below the router floor,
// counting calls. fromReplica flips the entry's replica provenance so we can
// prove the directory caches a primary version-gate answer but not a replica
// one (which may be lagged behind the primary's true version).
type gateCat struct {
	calls       int
	fromReplica bool
}

func (c *gateCat) Get(_ context.Context, id string) (catalog.Entry, error) {
	c.calls++
	return catalog.Entry{
		TenantID: id, ClusterID: "c1", Database: "fabriq_" + id,
		State: catalog.StateActive, Version: "1", FromReplica: c.fromReplica,
	}, nil
}
func (c *gateCat) Put(context.Context, catalog.Entry) (catalog.Entry, error) {
	return catalog.Entry{}, nil
}
func (c *gateCat) List(context.Context, catalog.Cursor, int) ([]catalog.Entry, catalog.Cursor, error) {
	return nil, "", nil
}

// A version-gated read served from a REPLICA during a primary outage must be
// degraded (non-cacheable): the replica's version may lag the primary's true
// (upgraded) version, so pinning CodeUnavailable for a TTL would keep a healthy
// tenant dark past primary recovery.
func TestCatalogDirectory_ReplicaVersionGate_NotCached(t *testing.T) {
	cat := &gateCat{fromReplica: true}
	now := time.Now()
	dir := CatalogDirectory(cat, time.Minute,
		WithCatalogClock(func() time.Time { return now }), WithMinVersion("2"))

	for i := 0; i < 3; i++ {
		if _, err := dir.Shard(context.Background(), "acme"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
			t.Fatalf("want CodeUnavailable, got %v", err)
		}
	}
	if cat.calls != 3 {
		t.Fatalf("version-gated replica read must not be cached: got %d calls, want 3", cat.calls)
	}
}

// A version-gated read served from the PRIMARY (authoritative) is still cached
// — pinning it for a TTL is correct and avoids a query storm for a genuinely
// schema-behind tenant.
func TestCatalogDirectory_PrimaryVersionGate_Cached(t *testing.T) {
	cat := &gateCat{fromReplica: false}
	now := time.Now()
	dir := CatalogDirectory(cat, time.Minute,
		WithCatalogClock(func() time.Time { return now }), WithMinVersion("2"))

	for i := 0; i < 3; i++ {
		if _, err := dir.Shard(context.Background(), "acme"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
			t.Fatalf("want CodeUnavailable, got %v", err)
		}
	}
	if cat.calls != 1 {
		t.Fatalf("primary version-gate answer must be cached: got %d calls, want 1", cat.calls)
	}
}

// The same rule applies to a not-active state served from a replica: it may be
// stale (the primary could have reactivated the tenant), so it must not pin.
func TestCatalogDirectory_ReplicaNotActive_NotCached(t *testing.T) {
	cat := &stateCat{state: catalog.StateSuspended, fromReplica: true}
	now := time.Now()
	dir := CatalogDirectory(cat, time.Minute, WithCatalogClock(func() time.Time { return now }))

	for i := 0; i < 3; i++ {
		if _, err := dir.Shard(context.Background(), "acme"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
			t.Fatalf("want CodeUnavailable, got %v", err)
		}
	}
	if cat.calls != 3 {
		t.Fatalf("not-active replica read must not be cached: got %d calls, want 3", cat.calls)
	}
}

type stateCat struct {
	calls       int
	state       catalog.State
	fromReplica bool
}

func (c *stateCat) Get(_ context.Context, id string) (catalog.Entry, error) {
	c.calls++
	return catalog.Entry{
		TenantID: id, ClusterID: "c1", Database: "fabriq_" + id,
		State: c.state, FromReplica: c.fromReplica,
	}, nil
}
func (c *stateCat) Put(context.Context, catalog.Entry) (catalog.Entry, error) {
	return catalog.Entry{}, nil
}
func (c *stateCat) List(context.Context, catalog.Cursor, int) ([]catalog.Entry, catalog.Cursor, error) {
	return nil, "", nil
}
