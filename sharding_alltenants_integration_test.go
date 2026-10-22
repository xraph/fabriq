//go:build integration

package fabriq_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestAllTenants_StaticUnionsRegistry: in static mode AllTenants is the sorted
// union of the tenant registry (a and c, which never wrote) and the tenants
// with events (b, which is not registered).
func TestAllTenants_StaticUnionsRegistry(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	fabriqtest.ApplyDDL(t, superDSN, []string{
		`INSERT INTO fabriq_tenant (id) VALUES ('a'), ('c')`,
	})
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: appDSN}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	bctx, err := tenant.WithTenant(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Exec(bctx, command.Command{Entity: "site", Op: command.OpCreate,
		Payload: &domain.Site{Name: "Plant b"}}); err != nil {
		t.Fatalf("Exec(site) for b: %v", err)
	}

	got, err := stores.AllTenants(ctx)
	if err != nil {
		t.Fatalf("AllTenants: %v", err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("AllTenants = %v, want %v", got, want)
	}
}

// TestAllTenants_StaticWithoutTenantTable: a database that has not run the
// migration creating fabriq_tenant keeps the old behaviour, the tenants with
// events and no error, so the worker's reconcile and blob GC loops keep going
// while a rollout runs the binary ahead of `fabriq migrate up`.
func TestAllTenants_StaticWithoutTenantTable(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeFn() })
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Roll back, bounded, until fabriq_tenant is gone, without hard-coding
	// how many migrations sit above it.
	for i := 0; ; i++ {
		if n := fabriqtest.QueryStrings(t, superDSN,
			`SELECT table_name FROM information_schema.tables WHERE table_name = 'fabriq_tenant'`); len(n) == 0 {
			break
		}
		if i == 20 {
			t.Fatal("fabriq_tenant still present after 20 rollbacks")
		}
		rb, err := orch.Rollback(ctx)
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if len(rb.Rollback) == 0 {
			t.Fatal("ran out of migrations before fabriq_tenant was dropped")
		}
	}
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: appDSN}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	bctx, err := tenant.WithTenant(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Exec(bctx, command.Command{Entity: "site", Op: command.OpCreate,
		Payload: &domain.Site{Name: "Plant b"}}); err != nil {
		t.Fatalf("Exec(site) for b: %v", err)
	}

	got, err := stores.AllTenants(ctx)
	if err != nil {
		t.Fatalf("AllTenants without fabriq_tenant: %v", err)
	}
	if want := []string{"b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("AllTenants = %v, want %v", got, want)
	}
}
