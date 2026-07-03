//go:build integration

package migrations_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func openPG(t *testing.T) *pgdriver.PgDB {
	t.Helper()
	dsn := fabriqtest.StartPostgres(t)
	db := pgdriver.New()
	if err := db.Open(context.Background(), dsn); err != nil {
		t.Fatalf("open pg: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrate_UpStatusDown(t *testing.T) {
	db := openPG(t)
	ctx := context.Background()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}

	res, err := orch.Migrate(ctx)
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if len(res.Applied) != len(migrations.Group().Migrations()) {
		t.Fatalf("applied %d of %d migrations", len(res.Applied), len(migrations.Group().Migrations()))
	}

	statuses, err := orch.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range statuses {
		if len(g.Pending) != 0 {
			t.Fatalf("group %s still has %d pending after up", g.Name, len(g.Pending))
		}
	}

	// Idempotent: second run applies nothing.
	res2, err := orch.Migrate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Applied) != 0 {
		t.Fatalf("second Migrate applied %d, want 0", len(res2.Applied))
	}

	// Rollback removes the last migration.
	rb, err := orch.Rollback(ctx)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if len(rb.Rollback) == 0 {
		t.Fatal("Rollback rolled back nothing")
	}
}

func TestMigrate_CoreTablesExist(t *testing.T) {
	db := openPG(t)
	ctx := context.Background()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{
		"fabriq_outbox", "fabriq_projection_state", "fabriq_projection_applied",
		"fabriq_embeddings", "fabriq_crdt_updates", "fabriq_crdt_snapshots",
		// Renamed by 0030 (host-database coexistence).
		"fabriq_links", "fabriq_blob_objects", "fabriq_blob_cas",
		"fabriq_fs_nodes", "fabriq_digest_nodes",
	} {
		var n int
		row := db.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("check %s: %v", table, err)
		}
		if n != 1 {
			t.Errorf("table %s missing after migrate", table)
		}
	}

	// Demo tables were evicted from the chain (domain.DemoDDL owns them).
	for _, demo := range []string{"sites", "assets", "tags", "tag_readings"} {
		var n int
		row := db.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, demo)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("check %s: %v", demo, err)
		}
		if n != 0 {
			t.Errorf("demo table %s must not ship in the default chain", demo)
		}
	}
}

// TestDemoDDL_TablesHypertableAndRLS pins the evicted demo DDL: applying
// domain.DemoDDL on a migrated database yields the example tables with the
// standard tenant RLS, and tag_readings becomes a hypertable when the
// timescaledb extension is available.
func TestDemoDDL_TablesHypertableAndRLS(t *testing.T) {
	db := openPG(t)
	ctx := context.Background()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range domain.DemoDDL() {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("demo DDL: %v\n%s", err, stmt)
		}
	}

	var hyper int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'tag_readings'`).Scan(&hyper); err != nil {
		t.Fatalf("hypertable check: %v", err)
	}
	if hyper != 1 {
		t.Error("tag_readings is not a hypertable")
	}
	var rls bool
	if err := db.QueryRow(ctx, `SELECT relforcerowsecurity FROM pg_class WHERE relname = 'assets'`).Scan(&rls); err != nil {
		t.Fatal(err)
	}
	if !rls {
		t.Error("assets must FORCE ROW LEVEL SECURITY")
	}
}

// TestRegistryConformance applies all migrations and diffs
// information_schema against every registered entity spec — the bridge
// between the registry (runtime authority) and grove migrations (DDL
// authority). CI fails on drift in either direction.
func TestRegistryConformance(t *testing.T) {
	db := openPG(t)
	ctx := context.Background()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// The example entity tables are application-owned DDL now (evicted from
	// the shipped chain like pages); apply them so conformance can diff.
	for _, stmt := range domain.DemoDDL() {
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatalf("demo DDL: %v", err)
		}
	}

	for _, ent := range reg.All() {
		// Document-kind entities materialize to application-owned tables whose
		// DDL is deliberately NOT part of fabriq's shipped migration chain — a
		// library must never create a generically-named table (e.g. "pages")
		// in a host database. Their DDL authority is the application (see
		// domain.PagesDDL), applied by examples and the document-plane
		// integration tests, so they are out of scope for this registry↔grove
		// migrations conformance check.
		if ent.Spec.Kind == registry.KindDocument {
			continue
		}

		dbCols := map[string]bool{}
		rows, err := db.Query(ctx,
			`SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`,
			ent.Binding.Table)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatal(err)
			}
			dbCols[c] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()

		if len(dbCols) == 0 {
			t.Errorf("entity %q: table %s does not exist", ent.Spec.Name, ent.Binding.Table)
			continue
		}
		for _, col := range ent.Binding.Columns {
			if !dbCols[col] {
				t.Errorf("entity %q: model column %q missing from table %s (migration drift)",
					ent.Spec.Name, col, ent.Binding.Table)
			}
			delete(dbCols, col)
		}
		for extra := range dbCols {
			t.Errorf("entity %q: table %s has column %q not present on the model (model drift)",
				ent.Spec.Name, ent.Binding.Table, extra)
		}
	}
}
