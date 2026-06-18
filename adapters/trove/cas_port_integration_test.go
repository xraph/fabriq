//go:build integration

package trovestore_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestCASStoreSatisfiesBlobCAS(t *testing.T) {
	ctx := context.Background()

	// Bootstrap Postgres (ephemeral test instance).
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as superuser (schema owner).
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
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })

	// Build the CAS index over blob_cas.
	idx := trovestore.NewCASIndex(pg)

	// Set up an in-memory trove driver and ensure the bucket exists.
	drv := memdriver.New()
	if err := drv.Open(ctx, "mem://"); err != nil {
		t.Fatalf("memdriver.Open: %v", err)
	}
	tr, err := trove.Open(drv, trove.WithDefaultBucket("c"))
	if err != nil {
		t.Fatalf("trove.Open: %v", err)
	}
	if err := tr.CreateBucket(ctx, "c"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// NewCASStore takes the raw driver (not the *trove.Trove handle).
	cs := trovestore.NewCASStore(drv, idx, "c")

	// Compile-time assertion: CASStore satisfies blob.CAS.
	var _ blob.CAS = cs

	// Build a tenant context.
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant: %v", err)
	}

	// Store returns (hash, size, err) — fabriq-typed, no trove types exposed.
	hash, size, err := cs.Store(tctx, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if hash == "" {
		t.Fatal("Store: empty hash")
	}
	if size != 5 {
		t.Fatalf("Store: size = %d, want 5", size)
	}

	// Retrieve round-trips the bytes.
	rc, err := cs.Retrieve(tctx, hash)
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	_ = rc.Close()
	if string(got) != "hello" {
		t.Fatalf("Retrieve = %q, want %q", got, "hello")
	}
}
