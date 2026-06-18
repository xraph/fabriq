//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestTenantTxRawStampsTenant(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as schema owner (superuser).
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

	// Open as the restricted app role so RLS actually applies.
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	var got string
	if err := a.TenantTxRaw(tctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(`SELECT current_setting('app.tenant_id', true)`).Scan(tctx, &got)
	}); err != nil {
		t.Fatal(err)
	}
	if got != "acme" {
		t.Fatalf("app.tenant_id = %q, want acme", got)
	}
}
