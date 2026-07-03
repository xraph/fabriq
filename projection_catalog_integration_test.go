//go:build integration

package fabriq_test

// Catalog-mode projections end-to-end (db-per-tenant P5c): commands in TWO
// tenants' dedicated databases flow sweeper-relay -> shared Redis stream ->
// search engine -> shared Elasticsearch, with projection bookkeeping routed
// to (and physically living in) each tenant's own database, and Search()
// serving tenant-isolated hits through the one facade.

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

func TestCatalogMode_SearchProjectionAcrossTenantDBs(t *testing.T) {
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
	tenantDSNs := map[string]string{}
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatalf("provision %s: %v", tid, err)
		}
		tenantDSNs[tid], _ = ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tenantDSNs[tid], cmDDL())
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

	// The worker plane, wired the way forgeext wires it: the sweeper
	// relays every tenant's outbox; the search engine consumes the shared
	// stream with per-tenant routed bookkeeping.
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	sweeper := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		ScanInterval: 100 * time.Millisecond,
		MinVersion:   migrations.HeadVersion(),
		OnError:      func(tid string, err error) { t.Logf("sweep %s: %v", tid, err) },
	})
	go func() { _ = sweeper.Run(runCtx) }()
	engine, err := stores.SearchEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "catalog-e2e") }()

	// A widget per tenant, then wait for the projection per tenant — the
	// wait reads the ROUTED bookkeeping (ports.ProjectionState).
	ids := map[string]string{}
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		res, execErr := f.Exec(tctx, command.Command{
			Entity: "cmwidget", Op: command.OpCreate,
			Payload: &cmWidget{Name: "turbine-" + tid},
		})
		if execErr != nil {
			t.Fatalf("exec %s: %v", tid, execErr)
		}
		ids[tid] = res.AggID
	}
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		waitCtx, cancel := context.WithTimeout(tctx, 60*time.Second)
		if err := f.WaitForProjection(waitCtx, "search", "cmwidget", ids[tid], 1); err != nil {
			cancel()
			t.Fatalf("WaitForProjection(search, %s): %v", tid, err)
		}
		cancel()
	}

	// Each tenant finds ONLY its own widget through the shared facade.
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		var hits []map[string]any
		if err := f.Search().Search(tctx, query.SearchQuery{Entity: "cmwidget", Query: "turbine"}, &hits); err != nil {
			t.Fatalf("search %s: %v", tid, err)
		}
		if len(hits) != 1 || hits[0]["id"] != ids[tid] {
			t.Fatalf("%s hits = %v, want only its own widget", tid, hits)
		}
	}

	// The bookkeeping physically lives in each tenant's OWN database and
	// records only that tenant.
	for _, tid := range []string{"acme", "globex"} {
		rows := fabriqtest.QueryStrings(t, tenantDSNs[tid],
			`SELECT DISTINCT tenant_id FROM fabriq_projection_applied`)
		if len(rows) != 1 || rows[0] != tid {
			t.Fatalf("%s database bookkeeping tenants = %v, want only itself", tid, rows)
		}
	}
}
