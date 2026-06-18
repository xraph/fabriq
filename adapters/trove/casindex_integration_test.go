//go:build integration

package trovestore_test

import (
	"context"
	"errors"
	"testing"

	trovecas "github.com/xraph/trove/cas"

	"github.com/xraph/fabriq/adapters/postgres"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestCASIndexPerTenant(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as the superuser (schema owner).
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
	pg, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close() })

	idx := trovestore.NewCASIndex(pg)

	acme, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	other, err := tenant.WithTenant(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}

	// Put an entry for acme.
	if err := idx.Put(acme, &trovecas.Entry{Hash: "h1", Bucket: "b", Key: "k", Size: 5, RefCount: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get returns the entry with the initial ref_count.
	got, err := idx.Get(acme, "h1")
	if err != nil || got == nil {
		t.Fatalf("acme Get: got=%+v err=%v", got, err)
	}
	if got.RefCount != 1 {
		t.Fatalf("acme Get RefCount = %d, want 1", got.RefCount)
	}

	// IncrementRef bumps ref_count by 1.
	if err := idx.IncrementRef(acme, "h1"); err != nil {
		t.Fatalf("IncrementRef: %v", err)
	}
	g, err := idx.Get(acme, "h1")
	if err != nil {
		t.Fatalf("Get after IncrementRef: %v", err)
	}
	if g.RefCount != 2 {
		t.Fatalf("after IncrementRef RefCount = %d, want 2", g.RefCount)
	}

	// Per-tenant isolation: "other" cannot see acme's entry (RLS).
	if _, err := idx.Get(other, "h1"); !errors.Is(err, trovecas.ErrNotFound) {
		t.Fatalf("cross-tenant Get should be ErrNotFound, got %v", err)
	}
}
