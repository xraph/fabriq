//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestRegisteredTenants lists fabriq_tenant in id order, and on a database
// that has not run the migration creating the table yet it reports no
// registered tenants rather than an error, so callers keep working while a
// rollout runs the new binary ahead of `fabriq migrate up`.
func TestRegisteredTenants(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	a, err := postgres.Open(ctx, superDSN, registry.New())
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	orch, err := migrations.NewOrchestrator(a.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	fabriqtest.ApplyDDL(t, superDSN, []string{`INSERT INTO fabriq_tenant (id) VALUES ('b'), ('a')`})

	got, err := a.RegisteredTenants(ctx)
	if err != nil {
		t.Fatalf("RegisteredTenants: %v", err)
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("RegisteredTenants = %v, want %v", got, want)
	}

	// Roll back, one migration at a time and bounded, until the table is
	// gone, so the test does not depend on how many migrations follow it.
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

	got, err = a.RegisteredTenants(ctx)
	if err != nil {
		t.Fatalf("RegisteredTenants without the table: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("RegisteredTenants without the table = %v, want none", got)
	}
}
