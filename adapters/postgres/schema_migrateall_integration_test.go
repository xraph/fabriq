//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestMigrateAll_SchemasInOneDB proves the schema-mode fleet roller end-to-end
// against real Postgres: a SchemaProvisioner rolls fabriq's chain across
// several tenant schemas sharing one consolidation database, recording each
// schema's head version in the catalog.
func TestMigrateAll_SchemasInOneDB(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	db := databaseOf(t, superDSN)
	ops := postgres.NewClusterOps(map[string]string{"c1": superDSN})
	cat := fabriqtest.NewFakeCatalog()
	sp := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")

	// Provision three tenants into one consolidation database.
	tenants := []string{"acme", "beta", "gamma"}
	for _, id := range tenants {
		if _, err := sp.Provision(ctx, id, "c1", db); err != nil {
			t.Fatalf("provision %s: %v", id, err)
		}
	}

	// Clear recorded versions so the roll has work to record, then roll.
	for _, id := range tenants {
		e, _ := cat.Get(ctx, id)
		e.Version = "0"
		if _, err := cat.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := sp.MigrateAll(ctx, provision.MigrateAllOpts{Batch: 2})
	if err != nil {
		t.Fatalf("migrate-all: %v", err)
	}
	if rep.Migrated != len(tenants) {
		t.Fatalf("migrated %d, want %d (%+v)", rep.Migrated, len(tenants), rep.Results)
	}

	// Every tenant now records the binary's head version.
	for _, id := range tenants {
		e, err := cat.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if e.State != catalog.StateActive || e.Version != migrations.HeadVersion() {
			t.Fatalf("%s: state=%s version=%s (want active/%s)", id, e.State, e.Version, migrations.HeadVersion())
		}
	}
}
