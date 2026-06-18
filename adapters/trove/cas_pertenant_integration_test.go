//go:build integration

package trovestore_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/xraph/trove/drivers/memdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// migrateAppCAS boots an ephemeral Postgres, runs fabriq migrations as owner,
// then returns an app-role adapter (RLS active) plus a fresh mem CAS store.
func migrateAppCAS(t *testing.T) (*postgres.Adapter, *trovestore.CASStore) {
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

	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
		t.Fatalf("memdriver.Open: %v", err)
	}
	// NewCASStore now treats "c" as the BASE; forTenant derives per-tenant buckets.
	cs := trovestore.NewCASStore(drv, trovestore.NewCASIndex(pg), "c")
	return pg, cs
}

func TestCASStorePerTenantIsolation(t *testing.T) {
	ctx := context.Background()
	_, cs := migrateAppCAS(t)

	tctxA, err := tenant.WithTenant(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tctxB, err := tenant.WithTenant(ctx, "tenant-b")
	if err != nil {
		t.Fatal(err)
	}

	// Both tenants store IDENTICAL content.
	hA, _, err := cs.Store(tctxA, bytes.NewReader([]byte("shared-bytes")))
	if err != nil {
		t.Fatalf("A Store: %v", err)
	}
	hB, _, err := cs.Store(tctxB, bytes.NewReader([]byte("shared-bytes")))
	if err != nil {
		t.Fatalf("B Store: %v", err)
	}
	// Same content → same content hash (CAS is deterministic).
	if hA != hB {
		t.Fatalf("hash mismatch for identical content: %q vs %q", hA, hB)
	}

	// Each tenant can retrieve its own copy.
	for name, tctx := range map[string]context.Context{"A": tctxA, "B": tctxB} {
		rc, err := cs.Retrieve(tctx, hA)
		if err != nil {
			t.Fatalf("%s Retrieve: %v", name, err)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		if string(got) != "shared-bytes" {
			t.Fatalf("%s Retrieve = %q, want shared-bytes", name, got)
		}
	}

	// Require fails without a tenant in context.
	if _, _, err := cs.Store(ctx, bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("Store without tenant context should error")
	}
}
