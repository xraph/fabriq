//go:build integration

package adminapi

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newTenantRegistryDB runs the full migration chain on a real Postgres and
// returns a TenantRegistry over the restricted app role's grove, like
// newKeyStoreDB does for the key store.
func newTenantRegistryDB(t *testing.T) TenantRegistry {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = owner.Close()

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	return NewTenantRegistry(a.Grove())
}

func TestTenantRegistry_PG(t *testing.T) {
	ctx := context.Background()
	tr := newTenantRegistryDB(t)

	if got, err := tr.List(ctx); err != nil || len(got) != 0 {
		t.Fatalf("empty List = %v, %v; want no records", got, err)
	}
	if _, found, err := tr.Get(ctx, "acme"); err != nil || found {
		t.Fatalf("Get unknown = found %v, err %v; want not found", found, err)
	}

	first, created, err := tr.Create(ctx, "zeta", "Zeta Co")
	if err != nil || !created {
		t.Fatalf("Create zeta = %+v, created %v, err %v", first, created, err)
	}
	if first.ID != "zeta" || first.Label != "Zeta Co" || first.CreatedAt.IsZero() {
		t.Fatalf("Create zeta record = %+v", first)
	}
	if _, created, err := tr.Create(ctx, "acme", "Acme"); err != nil || !created {
		t.Fatalf("Create acme: created %v, err %v", created, err)
	}

	got, found, err := tr.Get(ctx, "zeta")
	if err != nil || !found {
		t.Fatalf("Get zeta: found %v, err %v", found, err)
	}
	if got.ID != "zeta" || got.Label != "Zeta Co" || !got.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("Get zeta = %+v, want %+v", got, first)
	}

	list, err := tr.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 || list[0].ID != "acme" || list[1].ID != "zeta" {
		t.Fatalf("List = %+v, want [acme zeta] ordered by id", list)
	}

	// An idempotent create keeps the first label and created_at.
	time.Sleep(10 * time.Millisecond)
	again, created, err := tr.Create(ctx, "zeta", "Another Label")
	if err != nil {
		t.Fatalf("Create zeta again: %v", err)
	}
	if created {
		t.Error("second Create reported created = true")
	}
	if again.Label != "Zeta Co" || !again.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("second Create = %+v, want label and created_at of the first (%+v)", again, first)
	}
}
