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

func migrateAppIndex(t *testing.T) *trovestore.CASIndex {
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
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	pg, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	return trovestore.NewCASIndex(pg)
}

func TestCASIndexMutationsErrNotFound(t *testing.T) {
	ctx := context.Background()
	idx := migrateAppIndex(t)
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Every mutation on an absent hash returns trovecas.ErrNotFound.
	mutations := map[string]func() error{
		"Delete":       func() error { return idx.Delete(tctx, "missing") },
		"IncrementRef": func() error { return idx.IncrementRef(tctx, "missing") },
		"DecrementRef": func() error { return idx.DecrementRef(tctx, "missing") },
		"Pin":          func() error { return idx.Pin(tctx, "missing") },
		"Unpin":        func() error { return idx.Unpin(tctx, "missing") },
	}
	for name, fn := range mutations {
		if err := fn(); !errors.Is(err, trovecas.ErrNotFound) {
			t.Fatalf("%s(missing) = %v, want trovecas.ErrNotFound", name, err)
		}
	}

	// On a present hash the mutation succeeds (no false ErrNotFound).
	if err := idx.Put(tctx, &trovecas.Entry{Hash: "h", Bucket: "b", Key: "h", Size: 1, RefCount: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := idx.IncrementRef(tctx, "h"); err != nil {
		t.Fatalf("IncrementRef(present) = %v, want nil", err)
	}
}
