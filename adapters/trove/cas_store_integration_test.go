//go:build integration

package trovestore_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/xraph/trove"
	"github.com/xraph/trove/drivers/memdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestCASStoreDedup(t *testing.T) {
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
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	pg, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })

	// Build the CAS index over fabriq_blob_cas.
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
	// CreateBucket is idempotent; the driver must have the bucket before Store.
	if err := tr.CreateBucket(ctx, "c"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// NewCASStore takes the raw driver (not the *trove.Trove handle).
	cs := trovestore.NewCASStore(drv, idx, "c")

	// Build a tenant context.
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant: %v", err)
	}

	// First store.
	h1, _, err := cs.Store(tctx, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("first Store: %v", err)
	}

	// Second store of identical content.
	h2, _, err := cs.Store(tctx, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("second Store: %v", err)
	}

	// Invariant 1: same content → same hash.
	if h1 != h2 {
		t.Fatalf("dedup broken: same bytes produced different hashes %q vs %q", h1, h2)
	}

	// Invariant 2: dedup increments ref_count (ON CONFLICT ... ref_count + 1).
	e, err := idx.Get(tctx, h1)
	if err != nil {
		t.Fatalf("idx.Get(%q): %v", h1, err)
	}
	t.Logf("dedup: hash=%s ref_count=%d", h1, e.RefCount)
	if e.RefCount < 2 {
		t.Fatalf("dedup did not increment ref_count: got %d, want >= 2", e.RefCount)
	}
}
