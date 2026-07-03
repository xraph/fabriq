//go:build integration

package postgres_test

// End-to-end provisioning on a real cluster: the state machine creates
// dedicated databases, migrates them to head, records versions, and stays
// idempotent — the P4 contract on real Postgres.

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestProvision_RealClusterCreatesAndMigrates(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t) // doubles as control DB + cluster maintenance DSN

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)

	for _, tenant := range []string{"acme", "globex"} {
		entry, err := p.Provision(ctx, tenant, "c1")
		if err != nil {
			t.Fatalf("provision %s: %v", tenant, err)
		}
		if entry.State != catalog.StateActive || entry.Version != migrations.HeadVersion() {
			t.Fatalf("%s entry = %+v (head %s)", tenant, entry, migrations.HeadVersion())
		}
	}

	// Both databases exist with fabriq's chain applied (spot-check a
	// namespaced table inside each tenant database).
	for _, db := range []string{"fabriq_acme", "fabriq_globex"} {
		tenantDSN, err := ops.TenantDSN("c1", db)
		if err != nil {
			t.Fatal(err)
		}
		tables := fabriqtest.QueryStrings(t, tenantDSN,
			`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'fabriq_outbox'`)
		if len(tables) != 1 {
			t.Fatalf("%s: fabriq_outbox missing after provisioning", db)
		}
	}

	// Idempotent re-run and fleet roll over an already-current fleet.
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatalf("re-provision: %v", err)
	}
	report, err := p.MigrateAll(ctx, provision.MigrateAllOpts{TargetVersion: migrations.HeadVersion()})
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 2 || report.Failed != 0 {
		t.Fatalf("roll report = %+v", report)
	}
}
