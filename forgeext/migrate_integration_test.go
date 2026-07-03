//go:build integration

package forgeext_test

import (
	"context"
	"testing"

	"github.com/xraph/forge"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

func TestExtension_MigratableExtension(t *testing.T) {
	ctx := context.Background()

	// Boot a fresh Postgres container — migrations have NOT been run yet.
	superDSN := fabriqtest.StartPostgres(t)

	// Build registry (domain entities registered; orchestrator uses fabriq's
	// own migration group regardless).
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("domain.RegisterAll: %v", err)
	}

	ext := forgeext.New(reg, forgeext.WithConfig(fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
	}))

	// Verify the extension satisfies forge.MigratableExtension at compile time.
	var _ forge.MigratableExtension = ext

	// -------------------------------------------------------------------------
	// Migrate forward.
	// -------------------------------------------------------------------------
	res, err := ext.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if res.Applied == 0 {
		t.Fatalf("Migrate: expected Applied > 0, got %d", res.Applied)
	}
	t.Logf("Migrate: Applied=%d, Names=%v", res.Applied, res.Names)

	// -------------------------------------------------------------------------
	// Verify tables exist in Postgres using pgdriver (consistent with
	// migrations_integration_test.go which uses the same driver).
	// -------------------------------------------------------------------------
	db := pgdriver.New()
	if err := db.Open(ctx, superDSN); err != nil {
		t.Fatalf("pgdriver.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// fabriq_links is one of the 0029-renamed infra tables — asserting it
	// (rather than a demo table; those were evicted to domain.DemoDDL)
	// proves the chain ran through the namespace rename.
	for _, table := range []string{"fabriq_outbox", "fabriq_links"} {
		var n int
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = $1`, table,
		).Scan(&n); err != nil {
			t.Fatalf("checking table %q: %v", table, err)
		}
		if n != 1 {
			t.Errorf("expected table %q to exist after Migrate, but it does not", table)
		}
	}

	// -------------------------------------------------------------------------
	// MigrationStatus after full forward migration.
	// -------------------------------------------------------------------------
	groups, err := ext.MigrationStatus(ctx)
	if err != nil {
		t.Fatalf("MigrationStatus: %v", err)
	}
	if len(groups) == 0 {
		t.Fatal("MigrationStatus: expected at least one group, got none")
	}

	var foundGroup *forge.MigrationGroupInfo
	for _, g := range groups {
		if g.Name == migrations.GroupName {
			foundGroup = g
			break
		}
	}
	if foundGroup == nil {
		names := make([]string, len(groups))
		for i, g := range groups {
			names[i] = g.Name
		}
		t.Fatalf("MigrationStatus: group %q not found; got groups: %v", migrations.GroupName, names)
	}
	if len(foundGroup.Applied) == 0 {
		t.Errorf("MigrationStatus: expected Applied > 0 for group %q, got 0", migrations.GroupName)
	}
	if len(foundGroup.Pending) != 0 {
		t.Errorf("MigrationStatus: expected Pending = 0 for group %q after full migrate, got %d",
			migrations.GroupName, len(foundGroup.Pending))
	}
	t.Logf("MigrationStatus: group=%q Applied=%d Pending=%d",
		foundGroup.Name, len(foundGroup.Applied), len(foundGroup.Pending))

	// -------------------------------------------------------------------------
	// Rollback: grove rolls back exactly one migration (the most recently
	// applied). Assert no error; RolledBack is 0 or 1.
	// -------------------------------------------------------------------------
	rRes, err := ext.Rollback(ctx)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if rRes.RolledBack < 0 {
		t.Errorf("Rollback: unexpected negative RolledBack=%d", rRes.RolledBack)
	}
	t.Logf("Rollback: RolledBack=%d, Names=%v", rRes.RolledBack, rRes.Names)
}
