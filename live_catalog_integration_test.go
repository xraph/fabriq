//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// Live queries serve per-tenant in catalog mode: a subscription seeded from
// the tenant's own database, isolated from the neighbor tenant.
func TestCatalogMode_LiveQueryPerTenant(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatal(err)
		}
		tdsn, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tdsn, cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
		Redis:   fabriq.RedisConfig{Addr: redisAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// Seed one widget per tenant.
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		if _, err := f.Exec(tctx, command.Command{
			Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "live-" + tid},
		}); err != nil {
			t.Fatalf("exec %s: %v", tid, err)
		}
	}

	// A live query for acme is seeded from acme's DB only: LiveQuery takes an
	// RLS-enforced snapshot synchronously and returns it alongside the live
	// delta channel and a Handle to tear the subscription down.
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	lq := livequery.LiveQuery{Entity: "cmwidget", Limit: 10}

	snap, _, handle, err := f.LiveQuery(acmeCtx, lq)
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer handle.Close()

	if len(snap.Rows) != 1 {
		t.Fatalf("acme snapshot rows = %d, want its single widget", len(snap.Rows))
	}
}
