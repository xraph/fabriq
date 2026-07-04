//go:build integration

package fabriq_test

// Chaos test proving catalog-mode routing continues via a physical read
// replica during a catalog-PRIMARY outage (Phase 1: catalog read-replica
// failover). The catalog control DB lives on a postgres:16-alpine
// primary/standby pair reached through a repointable write-proxy; tenant
// DBs live on a separate timescaledb-ha cluster (they need pgvector/
// timescale) and are unaffected by the catalog outage.

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// waitReplicated blocks until the catalog row for tenant is visible on the
// (physical) standby, proving the provisioning write reached the replica.
func waitReplicated(t *testing.T, standbyDSN, tenantID string) {
	t.Helper()
	for i := 0; i < 80; i++ {
		rows := fabriqtest.QueryStrings(t, standbyDSN,
			`SELECT tenant_id FROM fabriq_tenant_catalog WHERE tenant_id = $1`, tenantID)
		if len(rows) == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("catalog row for %q never replicated to the standby", tenantID)
}

func TestChaosHA_RoutingContinuesViaReplica(t *testing.T) {
	ctx := context.Background()
	catPrimaryDSN, catStandbyDSN, proxy := fabriqtest.StartPrimaryStandby(t)
	tenantClusterDSN := fabriqtest.StartPostgres(t)

	// Provision two tenants: catalog rows land on the catalog primary (via the
	// proxy) and replicate to the standby; tenant DBs are created on the
	// separate timescaledb-ha cluster.
	cat, err := postgres.OpenCatalog(ctx, proxy.DSN(catPrimaryDSN))
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": tenantClusterDSN})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "beta"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatal(err)
		}
		tdsn, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tdsn, cmDDL())
	}
	_ = cat.Close()
	waitReplicated(t, catStandbyDSN, "beta")

	f, stores, err := fabriq.Open(ctx, cmRegistry(t), fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            proxy.DSN(catPrimaryDSN),
			ReplicaDSNs:    []string{catStandbyDSN},
			ClusterDSNs:    map[string]string{"c1": tenantClusterDSN},
			CacheTTL:       time.Minute,
			AllowSuperuser: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// Warm acme's route + shard (so it is cached before the outage).
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	widget := command.Command{Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "w"}}
	if _, err := f.Exec(acmeCtx, widget); err != nil {
		t.Fatal(err)
	}

	// Kill the catalog PRIMARY: the write endpoint (proxy) goes dark; the
	// standby (ReplicaDSNs, reached directly) is unaffected.
	proxy.Repoint("127.0.0.1:1")

	// beta was never cached — it must still route, served by the replica.
	betaCtx, _ := tenant.WithTenant(ctx, "beta")
	if _, err := f.Exec(betaCtx, widget); err != nil {
		t.Fatalf("uncached tenant must route via the replica during a catalog-primary outage: %v", err)
	}
	// acme's cached route also keeps serving.
	if _, err := f.Exec(acmeCtx, widget); err != nil {
		t.Fatalf("cached route must keep serving during the outage: %v", err)
	}
}
