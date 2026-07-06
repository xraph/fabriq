//go:build integration

package fabriq_test

// Catalog-mode analytics: the cross-tenant sink must be dialed by
// openCatalogMode exactly like the static/sharded Open path (see
// open.go's shared openAnalytics helper). Before this fix, catalog mode had
// its own assembly function that never touched cfg.Analytics, so
// stores.Analytics stayed nil forever in db-per-tenant deployments.

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestCatalogMode_AnalyticsSinkDialed proves the floor: a catalog-mode Open
// with Config.Analytics.DSN set produces a non-nil stores.Analytics, so the
// proj:analytics consumer and the CLI/admin backfiller both have something to
// drive. It also provisions one tenant so the config represents a realistic
// catalog deployment.
func TestCatalogMode_AnalyticsSinkDialed(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)          // control DB + cluster maintenance DSN
	analyticsDSN := fabriqtest.StartPostgres(t) // separate physical database

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatalf("provision acme: %v", err)
	}
	_ = cat.Close()
	tenantDSN, derr := ops.TenantDSN("c1", "fabriq_acme")
	if derr != nil {
		t.Fatal(derr)
	}
	fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true, // testcontainers hand out superuser creds
		},
		Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	_ = f

	if stores.Analytics == nil {
		t.Fatal("catalog mode must dial the analytics sink when Config.Analytics is configured")
	}
}

// TestCatalogMode_AnalyticsDSNCollisionRejected proves ValidateAnalyticsConfig
// is actually wired into the catalog Open path (not just reachable in
// isolation): an analytics DSN equal to a catalog cluster DSN must fail
// catalog-mode Open, mirroring the guard added to ValidateAnalyticsConfig for
// Catalog.ClusterDSNs.
func TestCatalogMode_AnalyticsDSNCollisionRejected(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatalf("provision acme: %v", err)
	}
	_ = cat.Close()
	tenantDSN, derr := ops.TenantDSN("c1", "fabriq_acme")
	if derr != nil {
		t.Fatal(derr)
	}
	fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())

	reg := cmRegistry(t)
	_, _, err = fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true,
		},
		// Deliberately colliding: same physical DSN as the cluster.
		Analytics: fabriq.AnalyticsConfig{DSN: dsn},
	})
	if err == nil {
		t.Fatal("expected catalog-mode Open to reject an analytics DSN colliding with a cluster DSN")
	}
}
