package shard

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/core/tenant"
)

// TestSchemaMode_PoolsTrackConsolidationDBs proves the operational win of
// schema-per-tenant consolidation mode: many tenants that share a
// consolidation database (same shard id "{cluster}/{database}", differing only
// by schema) open exactly ONE pool — the pool manager pools per database, not
// per tenant. 1000 tenants across 10 consolidation DBs => 10 pools, never 1000.
func TestSchemaMode_PoolsTrackConsolidationDBs(t *testing.T) {
	const dbs, perDB = 10, 100
	d := newFakeDialer()
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: dbs}) // cap == #DBs
	dir := staticDirectory{}
	for db := 0; db < dbs; db++ {
		for i := 0; i < perDB; i++ {
			tid := fmt.Sprintf("t_%d_%d", db, i)
			dir[tid] = fmt.Sprintf("c1/pool_%d", db) // schema differs, shard id shared
		}
	}
	router := NewSchemaRouter(NewDynamicSet(dir, p))

	for db := 0; db < dbs; db++ {
		for i := 0; i < perDB; i++ {
			tid := fmt.Sprintf("t_%d_%d", db, i)
			ctx, err := tenant.WithTenant(context.Background(), tid)
			if err != nil {
				t.Fatal(err)
			}
			_, _, release, err := router.Acquire(ctx)
			if err != nil {
				t.Fatalf("acquire %s: %v", tid, err)
			}
			release() // release so the next tenant on this DB reuses the pool
		}
	}

	open, _ := p.Stats()
	if open != dbs {
		t.Fatalf("open pools = %d, want %d (one per consolidation database, not per tenant)", open, dbs)
	}
	// And each database dialed exactly once.
	for db := 0; db < dbs; db++ {
		if opens, _ := d.counts(fmt.Sprintf("c1/pool_%d", db)); opens != 1 {
			t.Fatalf("pool_%d dialed %d times, want 1", db, opens)
		}
	}
}

// TestSchemaMode_BlastRadius proves the documented tradeoff: a dead
// consolidation database opens its shard breaker for EVERY tenant that shares
// it (they route to the same shard id), while tenants on other consolidation
// databases are unaffected.
func TestSchemaMode_BlastRadius(t *testing.T) {
	d := newFakeDialer()
	d.fail["c1/dead"] = fmt.Errorf("connection refused")
	p := NewPoolManager(d.dial, PoolManagerConfig{MaxActive: 8})
	dir := staticDirectory{
		"a": "c1/dead", "b": "c1/dead", // two tenants share the dead DB
		"c": "c1/live", // a tenant on a healthy DB
	}
	router := NewSchemaRouter(NewDynamicSet(dir, p))
	acq := func(tid string) error {
		ctx, _ := tenant.WithTenant(context.Background(), tid)
		_, _, release, err := router.Acquire(ctx)
		if err == nil {
			release()
		}
		return err
	}
	if err := acq("a"); err == nil {
		t.Fatal("tenant a on the dead DB should fail")
	}
	if err := acq("b"); err == nil {
		t.Fatal("tenant b (sharing the dead DB) should also fail — shared blast radius")
	}
	if err := acq("c"); err != nil {
		t.Fatalf("tenant c on a healthy DB must be unaffected: %v", err)
	}
}
