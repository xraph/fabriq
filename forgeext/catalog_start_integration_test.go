//go:build integration

package forgeext_test

// A catalog-mode (db-per-tenant) forgeext.Extension must Start: the tenant
// catalog is the source of truth, so no primary Postgres DSN / shards / host
// grove is required. This closes the gap where Start's source-of-truth guard
// rejected catalog-only config, blocking the sweeper worker and the admin API
// from ever running in catalog mode.

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
)

func TestExtension_StartsInCatalogMode(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t) // control DB + single cluster

	// Provision one tenant so the catalog is non-empty (not required for
	// Start, but proves the wired provisioner reaches a real catalog).
	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	if _, err := provision.New(cat, ops).Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	_ = cat.Close()

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	ext := forgeext.New(reg, forgeext.WithConfig(fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true, // testcontainers hand out superuser creds
		},
	}))

	// The whole point: Start must NOT reject catalog-only config as
	// "a Postgres source of truth is required".
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("catalog-mode Start rejected: %v", err)
	}
	t.Cleanup(func() { _ = ext.Shutdown(ctx) })

	// The provisioner accessor resolves off the started catalog store.
	p := ext.Provisioner()
	if p == nil {
		t.Fatal("Provisioner() is nil after a catalog-mode Start")
	}
	entry, err := p.Provision(ctx, "acme", "c1") // idempotent: already active
	if err != nil {
		t.Fatalf("provisioner not wired to the catalog: %v", err)
	}
	if entry.TenantID != "acme" {
		t.Fatalf("entry = %+v", entry)
	}
}
