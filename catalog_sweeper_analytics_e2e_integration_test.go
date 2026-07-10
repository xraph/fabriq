//go:build integration

package fabriq_test

// Sweeper-driven catalog-mode analytics end-to-end (db-per-tenant): commands
// written into TWO tenants' dedicated databases flow through the real
// sweeper-relay -> shared Redis stream -> proj:analytics consumer -> the ONE
// shared analytics database, where both tenants' facts co-locate (the whole
// point of the cross-tenant sink) while each stays tagged by tenant_id.
//
// This is the fuller counterpart to catalogmode_analytics_integration_test.go
// (which only proves the sink is dialed): here the entire pipeline runs and a
// redacted fact is asserted to physically land in the analytics DB for each
// tenant, co-located, from separate per-tenant source databases.

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// cmRegistryAnalytics registers cmwidget analytics-marked (deny-by-default
// elsewhere): only the "name" field crosses the co-location trust boundary.
func cmRegistryAnalytics(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name: "cmwidget", Kind: registry.KindAggregate, Model: (*cmWidget)(nil),
		Analytics: &registry.AnalyticsSpec{Include: []string{"name"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestCatalogMode_SweeperAnalyticsAcrossTenantDBs(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)          // control DB + cluster maintenance DSN
	redisAddr := fabriqtest.StartRedis(t)       // the shared event stream
	analyticsDSN := fabriqtest.StartPostgres(t) // the ONE shared analytics database

	// Provision two tenants, each in its own dedicated database, and apply the
	// app-owned entity DDL inside each.
	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatalf("provision %s: %v", tid, err)
		}
		tenantDSN, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistryAnalytics(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog:   fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
		Redis:     fabriq.RedisConfig{Addr: redisAddr},
		Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	if stores.Analytics == nil {
		t.Fatal("catalog mode must dial the analytics sink")
	}

	// The worker plane, wired the way forgeext wires it in catalog mode: the
	// sweeper relays every active tenant's outbox to the shared stream, and the
	// proj:analytics consumer applies from that stream into the analytics DB.
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	sweeper := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		ScanInterval: 100 * time.Millisecond,
		MinVersion:   migrations.HeadVersion(),
		OnError:      func(tid string, err error) { t.Logf("sweep %s: %v", tid, err) },
	})
	go func() { _ = sweeper.Run(runCtx) }()
	cons, err := stores.AnalyticsConsumer(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = cons.Run(runCtx, "catalog-analytics-e2e") }()

	// A widget per tenant, written into that tenant's OWN database through the
	// one facade.
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

	// Poll the shared analytics DB until each tenant's redacted fact has landed
	// (sweeper relay + consumer apply are asynchronous). Assert semantically via
	// JSON operators — not on Postgres's JSONB text formatting — that the
	// allow-listed "name" carries the written value AND that it is the ONLY key
	// present (every other cmWidget column — id, tenant_id, version — was
	// stripped by redaction before crossing the trust boundary).
	for _, tid := range []string{"acme", "globex"} {
		deadline := time.Now().Add(60 * time.Second)
		var names []string
		for time.Now().Before(deadline) {
			names = fabriqtest.QueryStrings(t, analyticsDSN,
				`SELECT payload->>'name' FROM fabriq_analytics_facts
				 WHERE tenant_id = $1 AND aggregate = 'cmwidget' AND agg_id = $2`,
				tid, ids[tid])
			if len(names) == 1 {
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
		if len(names) != 1 {
			t.Fatalf("%s: fact never landed in the analytics DB (agg_id %s)", tid, ids[tid])
		}
		if names[0] != "turbine-"+tid {
			t.Fatalf("%s: fact name = %q, want %q", tid, names[0], "turbine-"+tid)
		}
		keys := fabriqtest.QueryStrings(t, analyticsDSN,
			`SELECT jsonb_object_keys(payload) FROM fabriq_analytics_facts
			 WHERE tenant_id = $1 AND aggregate = 'cmwidget' AND agg_id = $2 ORDER BY 1`,
			tid, ids[tid])
		if len(keys) != 1 || keys[0] != "name" {
			t.Fatalf("%s: fact payload keys = %v, want [name] only (redaction to the Include allow-list)", tid, keys)
		}
	}

	// Both tenants' facts co-locate in the ONE analytics database, each tagged
	// by its own tenant_id — the cross-tenant read model an operator scans.
	tenants := fabriqtest.QueryStrings(t, analyticsDSN,
		`SELECT tenant_id FROM fabriq_analytics_facts WHERE aggregate = 'cmwidget' ORDER BY tenant_id`)
	if len(tenants) != 2 || tenants[0] != "acme" || tenants[1] != "globex" {
		t.Fatalf("co-located analytics tenants = %v, want [acme globex]", tenants)
	}

	// The append-only event log likewise recorded both tenants' creates.
	events := fabriqtest.QueryStrings(t, analyticsDSN,
		`SELECT tenant_id FROM fabriq_analytics_events
		 WHERE aggregate = 'cmwidget' AND type = 'cmwidget.created' ORDER BY tenant_id`)
	if len(events) != 2 || events[0] != "acme" || events[1] != "globex" {
		t.Fatalf("analytics event-log tenants = %v, want [acme globex]", events)
	}
}
