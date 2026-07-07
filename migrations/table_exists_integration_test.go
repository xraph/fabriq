//go:build integration

package migrations_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestTableExists_SchemaAwareScopeColumns is the regression test for the
// migrations.tableExists fix: it hard-coded table_schema='public', so running
// the chain under a non-public search_path (schema-per-tenant consolidation
// mode) skipped the conditional CRDT scope_id ALTERs in migration 0013 — which
// then broke ApplyUpdate at runtime (INSERT into a column that was never
// added). tableExists is now search_path-aware, so the scope_id columns land
// in the tenant schema. This test runs the whole chain inside a schema and
// asserts the columns exist.
func TestTableExists_SchemaAwareScopeColumns(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	// Create the tenant schema up front.
	admin := pgdriver.New()
	if err := admin.Open(ctx, superDSN); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.Exec(ctx, `CREATE SCHEMA tenant_x`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	// Run the full migration chain with the connection's search_path pinned to
	// the tenant schema (the same libpq options trick MigrateSchema uses), so
	// every bare-named table — and every tableExists() probe — resolves there.
	u, err := url.Parse(superDSN)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("options", "-c search_path=tenant_x")
	u.RawQuery = q.Encode()

	db := pgdriver.New()
	if err := db.Open(ctx, u.String()); err != nil {
		t.Fatalf("open under schema: %v", err)
	}
	defer func() { _ = db.Close() }()
	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate under schema: %v", err)
	}

	// The scope_id columns (added by 0013 only when tableExists sees the CRDT
	// tables) must exist in the tenant schema. Before the fix, tableExists
	// looked only in 'public' and returned false, so these ALTERs were skipped.
	for _, table := range []string{"fabriq_crdt_updates", "fabriq_crdt_snapshots", "fabriq_crdt_docs"} {
		var n int
		if err := admin.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.columns
			 WHERE table_schema = 'tenant_x' AND table_name = $1 AND column_name = 'scope_id'`,
			table).Scan(&n); err != nil {
			t.Fatalf("check %s.scope_id: %v", table, err)
		}
		if n != 1 {
			t.Fatalf("tenant_x.%s is missing scope_id — tableExists skipped the 0013 ALTER under a non-public search_path", table)
		}
	}
}
