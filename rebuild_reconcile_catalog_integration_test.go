//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func openCatalogSearchFixture(t *testing.T) (context.Context, *fabriq.Fabriq, *fabriq.Stores, map[string]string) {
	t.Helper()
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	esURL := fabriqtest.StartElasticsearch(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	dsns := map[string]string{}
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatal(err)
		}
		dsns[tid], _ = ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, dsns[tid], cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog:       fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
		Redis:         fabriq.RedisConfig{Addr: redisAddr},
		Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{esURL}},
		Projections:   fabriq.ProjectionsConfig{Search: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	return ctx, f, stores, dsns
}

func TestCatalogMode_SearchRebuild(t *testing.T) {
	ctx, f, stores, _ := openCatalogSearchFixture(t)
	reg := f.Registry()

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	sw := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		ScanInterval: 100 * time.Millisecond, MinVersion: migrations.HeadVersion(),
	})
	go func() { _ = sw.Run(runCtx) }()
	engine, err := stores.SearchEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "rebuild-e2e") }()

	// Seed BOTH tenants so the rebuild's isolation is testable: acme gets
	// "turbine", globex gets "windmill", each in its own database.
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	res, err := f.Exec(acmeCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "turbine"},
	})
	if err != nil {
		t.Fatal(err)
	}
	globexCtx, _ := tenant.WithTenant(ctx, "globex")
	gres, err := f.Exec(globexCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "windmill"},
	})
	if err != nil {
		t.Fatal(err)
	}
	acmeWait, cancelA := context.WithTimeout(acmeCtx, 60*time.Second)
	if err := f.WaitForProjection(acmeWait, "search", "cmwidget", res.AggID, 1); err != nil {
		cancelA()
		t.Fatalf("acme initial projection: %v", err)
	}
	cancelA()
	globexWait, cancelG := context.WithTimeout(globexCtx, 60*time.Second)
	if err := f.WaitForProjection(globexWait, "search", "cmwidget", gres.AggID, 1); err != nil {
		cancelG()
		t.Fatalf("globex initial projection: %v", err)
	}
	cancelG()

	// Blue-green rebuild for acme ONLY: replay from acme's OWN database, swap
	// its alias, serving continues.
	rb, err := stores.SearchRebuilder(reg)
	if err != nil {
		t.Fatalf("SearchRebuilder must build in catalog mode: %v", err)
	}
	if _, _, err := rb.Rebuild(acmeCtx, "acme"); err != nil {
		t.Fatalf("Rebuild(acme): %v", err)
	}

	// acme's rebuilt index serves acme's row.
	var hits []map[string]any
	if err := f.Search().Search(acmeCtx, query.SearchQuery{Entity: "cmwidget", Query: "turbine"}, &hits); err != nil {
		t.Fatalf("search after rebuild: %v", err)
	}
	if len(hits) != 1 || hits[0]["id"] != res.AggID {
		t.Fatalf("post-rebuild acme hits = %v", hits)
	}

	// Isolation: rebuilding acme left globex's index untouched — globex still
	// finds its own row and never acme's.
	var gHits []map[string]any
	if err := f.Search().Search(globexCtx, query.SearchQuery{Entity: "cmwidget", Query: "windmill"}, &gHits); err != nil {
		t.Fatalf("globex search after acme rebuild: %v", err)
	}
	if len(gHits) != 1 || gHits[0]["id"] != gres.AggID {
		t.Fatalf("acme rebuild leaked into globex: %v", gHits)
	}
	var gCross []map[string]any
	if err := f.Search().Search(globexCtx, query.SearchQuery{Entity: "cmwidget", Query: "turbine"}, &gCross); err != nil {
		t.Fatalf("globex cross search: %v", err)
	}
	if len(gCross) != 0 {
		t.Fatalf("globex must not see acme's rebuilt row: %v", gCross)
	}
}

func TestCatalogMode_SearchReconcilerRepairsDrift(t *testing.T) {
	ctx, f, stores, tenantDSNs := openCatalogSearchFixture(t)
	reg := f.Registry()

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	sw := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		ScanInterval: 100 * time.Millisecond, MinVersion: migrations.HeadVersion(),
	})
	go func() { _ = sw.Run(runCtx) }()
	engine, err := stores.SearchEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "recon-e2e") }()

	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	res, err := f.Exec(acmeCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "drifter"},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(acmeCtx, 60*time.Second)
	if err := f.WaitForProjection(waitCtx, "search", "cmwidget", res.AggID, 1); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	// Simulate projection drift: bump the aggregate version directly in
	// acme's OWN database WITHOUT emitting an event, so ES is now stale.
	fabriqtest.ApplyDDL(t, tenantDSNs["acme"], []string{
		`UPDATE cm_widgets SET version = version + 1, name = 'repaired' WHERE id = '` + res.AggID + `'`,
	})

	// The reconciler compares truth (acme's DB) vs projected (shared ES) and
	// republishes a repair event onto acme's outbox; the sweeper relays it
	// and the engine re-applies.
	rec, err := stores.SearchReconciler(reg)
	if err != nil {
		t.Fatalf("SearchReconciler must build in catalog mode: %v", err)
	}
	if _, err := rec.Reconcile(ctx, "acme", true); err != nil {
		t.Fatalf("Reconcile(acme): %v", err)
	}
	waitCtx2, cancel2 := context.WithTimeout(acmeCtx, 60*time.Second)
	defer cancel2()
	if err := f.WaitForProjection(waitCtx2, "search", "cmwidget", res.AggID, 2); err != nil {
		t.Fatalf("drift not repaired to v2: %v", err)
	}
}
