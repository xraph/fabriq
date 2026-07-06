package fabriq

// Catalog-mode worker routing, unit-tested with fakes: projection
// bookkeeping routes through the DynamicSet's acquire/release discipline,
// and AllTenants reads the catalog instead of dialing databases.

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/fabriqtest"
)

// fakeStateStore records projection-state calls per shard.
type fakeStateStore struct {
	shardID string
	applied map[string]int64 // "tenant/proj/agg/id" -> version
}

func (f *fakeStateStore) AppliedVersion(_ context.Context, tenantID, proj, aggregate, aggID string) (int64, error) {
	return f.applied[tenantID+"/"+proj+"/"+aggregate+"/"+aggID], nil
}

func (f *fakeStateStore) SetApplied(_ context.Context, tenantID, proj, aggregate, aggID string, version int64) error {
	f.applied[tenantID+"/"+proj+"/"+aggregate+"/"+aggID] = version
	return nil
}

func (f *fakeStateStore) Get(_ context.Context, tenantID, proj string) (projection.State, error) {
	return projection.State{TenantID: tenantID, Projection: proj, TargetName: f.shardID}, nil
}

func (f *fakeStateStore) Upsert(context.Context, projection.State) error { return nil }
func (f *fakeStateStore) Tenants(context.Context) ([]string, error)      { return nil, nil }

// fakeRouter routes every tenant to its own fake shard and counts
// outstanding acquisitions (release discipline).
type fakeRouter struct {
	stores map[string]*fakeStateStore
	held   int
}

func (r *fakeRouter) Acquire(context.Context) (shard.Shard, context.Context, func(), error) {
	panic("worker routing must use AcquireFor")
}

func (r *fakeRouter) AcquireFor(ctx context.Context, tenantID string) (shard.Shard, context.Context, func(), error) {
	st, ok := r.stores[tenantID]
	if !ok {
		st = &fakeStateStore{shardID: "c1/fabriq_" + tenantID, applied: map[string]int64{}}
		r.stores[tenantID] = st
	}
	r.held++
	return shard.Shard{ID: st.shardID, Projection: st}, ctx, func() { r.held-- }, nil
}

func TestRoutingState_RoutesPerTenantThroughRouter(t *testing.T) {
	ctx := context.Background()
	router := &fakeRouter{stores: map[string]*fakeStateStore{}}
	stores := &Stores{router: router}
	stores.state = routingState{stores: stores}

	if err := stores.state.SetApplied(ctx, "acme", "search", "asset", "a1", 7); err != nil {
		t.Fatal(err)
	}
	if err := stores.state.SetApplied(ctx, "globex", "search", "asset", "a1", 3); err != nil {
		t.Fatal(err)
	}

	// Each tenant's bookkeeping landed on its OWN shard.
	if got := router.stores["acme"].applied["acme/search/asset/a1"]; got != 7 {
		t.Fatalf("acme applied = %d, want 7", got)
	}
	if got := router.stores["globex"].applied["globex/search/asset/a1"]; got != 3 {
		t.Fatalf("globex applied = %d, want 3", got)
	}

	v, err := stores.state.AppliedVersion(ctx, "acme", "search", "asset", "a1")
	if err != nil || v != 7 {
		t.Fatalf("AppliedVersion = (%d, %v), want 7", v, err)
	}
	st, err := stores.state.Get(ctx, "globex", "search")
	if err != nil || st.TargetName != "c1/fabriq_globex" {
		t.Fatalf("Get routed to %q (%v), want globex's shard", st.TargetName, err)
	}
	if err := stores.state.Upsert(ctx, projection.State{TenantID: "acme", Projection: "graph"}); err != nil {
		t.Fatal(err)
	}

	// Every acquisition was released (the pool must be free to evict).
	if router.held != 0 {
		t.Fatalf("%d shard acquisitions never released", router.held)
	}
}

func TestAllTenants_CatalogModeListsActiveEntries(t *testing.T) {
	ctx := context.Background()
	cat := fabriqtest.NewFakeCatalog()
	for _, e := range []catalog.Entry{
		{TenantID: "acme", ClusterID: "c1", Database: "fabriq_acme", State: catalog.StateActive},
		{TenantID: "globex", ClusterID: "c1", Database: "fabriq_globex", State: catalog.StateActive},
		{TenantID: "paused", ClusterID: "c1", Database: "fabriq_paused", State: catalog.StateSuspended},
	} {
		if _, err := cat.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	// Stores.AllTenants takes the catalog path whenever Catalog is set; the
	// concrete field type is the postgres store, so exercise the same page
	// walk through the interface seam it uses.
	tenants, err := allTenantsFromCatalog(ctx, cat)
	if err != nil {
		t.Fatal(err)
	}
	if len(tenants) != 2 || tenants[0] != "acme" || tenants[1] != "globex" {
		t.Fatalf("tenants = %v, want [acme globex]", tenants)
	}
}

// fakeReplay records which tenant's replay surface was used.
type fakeReplay struct {
	shardID  string
	versions map[string]int64
	repaired []string
}

func (f *fakeReplay) SnapshotEntities(_ context.Context, _ string, fn func(event.Envelope) error) error {
	return fn(event.Envelope{Aggregate: "cmwidget", AggID: f.shardID})
}
func (f *fakeReplay) AggregateVersions(_ context.Context, _, _ string) (map[string]int64, error) {
	return f.versions, nil
}
func (f *fakeReplay) Repair(_ context.Context, _ string, d projection.Drift) error {
	f.repaired = append(f.repaired, d.AggID)
	return nil
}

type fakeReplayRouter struct {
	stores map[string]*fakeReplay
	held   int
}

func (r *fakeReplayRouter) Acquire(context.Context) (shard.Shard, context.Context, func(), error) {
	panic("worker routing must use AcquireFor")
}
func (r *fakeReplayRouter) AcquireFor(ctx context.Context, tenantID string) (shard.Shard, context.Context, func(), error) {
	rs := r.stores[tenantID]
	r.held++
	return shard.Shard{ID: rs.shardID, Replay: rs}, ctx, func() { r.held-- }, nil
}

func TestReplaySource_RoutesPerTenant(t *testing.T) {
	ctx := context.Background()
	acme := &fakeReplay{shardID: "c1/fabriq_acme", versions: map[string]int64{"a1": 5}}
	globex := &fakeReplay{shardID: "c1/fabriq_globex", versions: map[string]int64{"b1": 9}}
	router := &fakeReplayRouter{stores: map[string]*fakeReplay{"acme": acme, "globex": globex}}
	stores := &Stores{router: router}

	v, err := stores.truthVersions(ctx, "globex", "cmwidget")
	if err != nil || v["b1"] != 9 {
		t.Fatalf("truthVersions = %v (%v), want globex's versions", v, err)
	}
	if err := stores.repair(ctx, "acme", projection.Drift{Entity: "cmwidget", AggID: "a1", TruthVersion: 5}); err != nil {
		t.Fatal(err)
	}
	if len(acme.repaired) != 1 || acme.repaired[0] != "a1" || len(globex.repaired) != 0 {
		t.Fatalf("repair routed wrong: acme=%v globex=%v", acme.repaired, globex.repaired)
	}
	if router.held != 0 {
		t.Fatalf("%d shard acquisitions never released", router.held)
	}
}
