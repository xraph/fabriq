//go:build integration

package postgres_test

// Phase 1 of the db-per-tenant design (spec 2026-07-03): fabriq's tables
// must be coexistence-safe inside a database shared with TwinOS and other
// Forge extensions (which prefix their tables). These tests pin:
//
//   - the default migration chain creates NO demo tables (sites, assets,
//     tags, tag_readings moved to domain.DemoDDL, applied by harnesses);
//   - every fabriq-owned table is namespaced (fabriq_* or ds_*);
//   - migration 0029 renames the previously-unprefixed infra tables and
//     isolation survives the rename (RLS policies follow renames; the
//     grove-hook backstop guards the new names).

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// migrateFresh runs the full default chain on a fresh container database
// and returns the super DSN.
func migrateFresh(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return superDSN
}

// listTables returns all ordinary tables in the public schema.
func listTables(t *testing.T, dsn string) map[string]bool {
	t.Helper()
	rows := fabriqtest.QueryStrings(t, dsn,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		out[r] = true
	}
	return out
}

func TestDefaultMigrations_NoDemoTables(t *testing.T) {
	dsn := migrateFresh(t)
	tables := listTables(t, dsn)
	for _, demo := range []string{"sites", "assets", "tags", "tag_readings"} {
		if tables[demo] {
			t.Errorf("default chain must not create demo table %q (moved to domain.DemoDDL)", demo)
		}
	}
}

func TestDefaultMigrations_AllTablesNamespaced(t *testing.T) {
	dsn := migrateFresh(t)
	for table := range listTables(t, dsn) {
		if strings.HasPrefix(table, "fabriq_") || strings.HasPrefix(table, "ds_") {
			continue
		}
		// Permitted outsiders: grove's migration bookkeeping and tables
		// owned by Postgres extensions themselves (e.g. PostGIS's
		// spatial_ref_sys) — they are not fabriq's to namespace.
		if strings.HasPrefix(table, "grove_") || strings.Contains(table, "migration") ||
			table == "spatial_ref_sys" {
			continue
		}
		t.Errorf("table %q is not namespaced (fabriq_*/ds_*) — clashes with host tables", table)
	}
}

func TestMigration0029_RenamesInfraTables(t *testing.T) {
	dsn := migrateFresh(t)
	tables := listTables(t, dsn)
	renamed := []string{
		"fabriq_fs_nodes", "fabriq_fs_permissions", "fabriq_fs_shares",
		"fabriq_fs_bookmarks", "fabriq_blob_objects", "fabriq_blob_cas",
		"fabriq_blob_sources", "fabriq_digest_nodes", "fabriq_links",
	}
	for _, want := range renamed {
		if !tables[want] {
			t.Errorf("missing renamed table %q", want)
		}
		old := strings.TrimPrefix(want, "fabriq_")
		if tables[old] {
			t.Errorf("old table name %q still present after rename", old)
		}
	}
}

func TestMigration0029_RLSFollowsRename(t *testing.T) {
	dsn := migrateFresh(t)
	// The renamed tables that carried FORCE RLS before the rename
	// (fabriq_blob_objects, fabriq_blob_cas, links, fabriq_digest_nodes; the fs_* tables and
	// fabriq_blob_sources are structurally isolated) must still FORCE it under
	// their new names — policies follow table renames in Postgres, and
	// this guards against a rewrite-style migration silently dropping them.
	rows := fabriqtest.QueryStrings(t, dsn, `
		SELECT c.relname FROM pg_class c
		WHERE c.relname IN ('fabriq_blob_objects','fabriq_blob_cas','fabriq_links','fabriq_digest_nodes')
		  AND c.relkind = 'r' AND NOT c.relforcerowsecurity`)
	for _, r := range rows {
		t.Errorf("renamed table %q lost FORCE ROW LEVEL SECURITY", r)
	}
	// And their tenant_isolation policies must still exist.
	pols := fabriqtest.QueryStrings(t, dsn, `
		SELECT tablename FROM pg_policies
		WHERE policyname = 'tenant_isolation'
		  AND tablename IN ('fabriq_blob_objects','fabriq_blob_cas','fabriq_links','fabriq_digest_nodes')`)
	if len(pols) != 4 {
		t.Errorf("tenant_isolation policies on renamed tables = %v, want all 4", pols)
	}
}

func TestDemoDDL_ProvidesEvictedTables(t *testing.T) {
	dsn := migrateFresh(t)
	fabriqtest.ApplyDDL(t, dsn, domain.DemoDDL())
	// Idempotent: harnesses may apply it more than once.
	fabriqtest.ApplyDDL(t, dsn, domain.DemoDDL())
	tables := listTables(t, dsn)
	for _, demo := range []string{"sites", "assets", "tags", "tag_readings"} {
		if !tables[demo] {
			t.Errorf("domain.DemoDDL must create %q", demo)
		}
	}
}

// BenchmarkRLS_StampedTxOverhead documents the cost of the FORCE RLS
// policy we deliberately keep even in single-tenant databases (defense in
// depth for the db-per-tenant topology): a stamped-transaction point read
// against an RLS-guarded renamed table. Compare the reported time against
// the same read with the policy dropped to quantify the delta (expected:
// noise — the policy is one equality check on a session variable).
func BenchmarkRLS_StampedTxOverhead(b *testing.B) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(b)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		b.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		b.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	fabriqtest.ApplyDDL(b, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(b, superDSN)
	app, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = app.Close() }()

	tctx, _ := tenant.WithTenant(ctx, "bench")
	docID := "link-bench"
	fabriqtest.ApplyDDL(b, superDSN, []string{
		`INSERT INTO fabriq_links (id, tenant_id, version, kind, source_id, target_id, note)
		 VALUES ('` + docID + `', 'bench', 1, 'rel', 'a1', 'b2', '')
		 ON CONFLICT (id) DO NOTHING`,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got domain.Link
		if err := app.Get(tctx, "link", docID, &got); err != nil {
			b.Fatal(err)
		}
	}
}
