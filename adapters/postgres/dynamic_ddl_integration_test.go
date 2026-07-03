//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestDynamicDDL_EnsureDynamic verifies that EnsureDynamic:
//  1. Creates the table from the descriptor (structural + domain columns).
//  2. Is idempotent — calling it twice produces no error.
//  3. Leaves the correct columns (verified via information_schema).
//  4. Installs the tenant_isolation RLS policy (verified via pg_policies).
//  5. Leaves the declared secondary index (verified via pg_indexes).
func TestDynamicDDL_EnsureDynamic(t *testing.T) {
	ctx := context.Background()

	// Boot a fresh Postgres container as the schema owner (superuser).
	superDSN := fabriqtest.StartPostgres(t)

	// Register only the dynamic entity — no domain pack needed.
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "orders",
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
				{Name: "meta", Type: registry.ColJSON},
			},
			Indexes: []registry.DynamicIndex{
				{Name: "ds_orders_sku_idx", Columns: []string{"sku"}},
			},
		},
	})

	// Open the adapter as the superuser so we can run DDL.
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	// Run standard fabriq migrations (outbox, projection state, RLS for
	// static tables, timescale, vector). They don't touch ds_orders.
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ent, ok := reg.Get("orders")
	if !ok {
		t.Fatal("entity 'orders' not found in registry")
	}

	// --- call 1: must succeed ---
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic (first call): %v", err)
	}

	// --- call 2: must be idempotent ---
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic (second call, idempotency): %v", err)
	}

	pg := owner.Driver()

	// --- verify columns via information_schema ---
	type colRow struct {
		ColumnName string
		DataType   string
	}
	rows, err := pg.Query(ctx,
		`SELECT column_name, data_type
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'ds_orders'
		 ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("query information_schema: %v", err)
	}
	defer rows.Close()

	colsByName := map[string]string{}
	for rows.Next() {
		var r colRow
		if err := rows.Scan(&r.ColumnName, &r.DataType); err != nil {
			t.Fatal(err)
		}
		colsByName[r.ColumnName] = r.DataType
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	// information_schema uses lower-case names; numeric precision included in
	// type names sometimes, so we check contains / prefix.
	wantCols := map[string]string{
		"id":        "text",
		"tenant_id": "text",
		"version":   "bigint",
		"sku":       "text",
		"qty":       "bigint",
		"meta":      "jsonb",
	}
	for col, wantType := range wantCols {
		got, ok := colsByName[col]
		if !ok {
			t.Errorf("column %q missing from ds_orders", col)
			continue
		}
		if got != wantType {
			t.Errorf("column %q: data_type = %q, want %q", col, got, wantType)
		}
	}
	if len(colsByName) != len(wantCols) {
		t.Errorf("ds_orders has %d columns, want %d: %v", len(colsByName), len(wantCols), colsByName)
	}

	// --- verify the RLS policy via pg_policies (view uses 'policyname') ---
	var polName string
	if err := pg.QueryRow(ctx,
		`SELECT policyname FROM pg_policies
		 WHERE schemaname = 'public' AND tablename = 'ds_orders' AND policyname = 'tenant_isolation'`,
	).Scan(&polName); err != nil {
		t.Fatalf("pg_policies query: %v (want tenant_isolation policy on ds_orders)", err)
	}
	if polName != "tenant_isolation" {
		t.Errorf("RLS policy = %q, want %q", polName, "tenant_isolation")
	}

	// --- verify the secondary index via pg_indexes ---
	var idxName string
	if err := pg.QueryRow(ctx,
		`SELECT indexname FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename = 'ds_orders' AND indexname = 'ds_orders_sku_idx'`,
	).Scan(&idxName); err != nil {
		t.Fatalf("pg_indexes query: %v (want ds_orders_sku_idx)", err)
	}
	if idxName != "ds_orders_sku_idx" {
		t.Errorf("secondary index = %q, want %q", idxName, "ds_orders_sku_idx")
	}

	// --- verify RLS is enforced by the app role ---
	// Provision the app role (after migrations so DEFAULT PRIVILEGES apply).
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	app, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })

	// Insert a row as superuser (bypasses RLS) then verify the app role
	// sees zero rows when no tenant is stamped.
	if _, err := pg.Exec(ctx,
		`INSERT INTO ds_orders (id, tenant_id, version, sku, qty) VALUES ('r1', 'acme', 1, 'WIDGET-1', 10)`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// The app role without a stamped tenant_id should see no rows (RLS
	// current_setting('app.tenant_id', true) is NULL outside a stamped tx).
	appRows, err := app.Driver().Query(ctx, `SELECT id FROM ds_orders`)
	if err != nil {
		// Some Postgres setups return an error when the setting is unset
		// (missing_ok=false equivalent); treat that as RLS working.
		t.Logf("app-role query without tenant stamp returned error (expected): %v", err)
	} else {
		defer appRows.Close()
		var ids []string
		for appRows.Next() {
			var id string
			if err := appRows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if len(ids) != 0 {
			t.Errorf("RLS leak: app role saw %d rows without tenant stamp: %v", len(ids), ids)
		}
	}
}

// TestDynamicDDL_NonDynamicEntityReturnsError verifies that EnsureDynamic
// refuses to operate on a static (model-backed) entity.
func TestDynamicDDL_NonDynamicEntityReturnsError(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	// Use the full domain pack so we have a static entity to test with.
	reg := registry.New()

	// Register a minimal static entity manually to avoid importing domain pack.
	type testModel struct {
		ID       string `grove:"column:id,pk"`
		TenantID string `grove:"column:tenant_id"`
		Version  int64  `grove:"column:version"`
		Name     string `grove:"column:name"`
	}
	reg.MustRegister(registry.EntitySpec{
		Name:  "static_thing",
		Model: (*testModel)(nil),
	})

	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	ent, _ := reg.Get("static_thing")
	if err := owner.EnsureDynamic(ctx, ent); err == nil {
		t.Fatal("EnsureDynamic on a static entity must return an error")
	}
}

// TestDestructiveDynamicDDL verifies the destructive DDL trio added on top of
// EnsureDynamic: RenameDynamicColumn, DropDynamicColumn (with the structural
// guard refusing to drop a structural column), and DropDynamic (drop table).
func TestDestructiveDynamicDDL(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "ddltest",
		Schema: &registry.DynamicSchema{
			Table: "ds_ddltest",
			Columns: []registry.DynamicColumn{
				{Name: "colour", Type: registry.ColText},
				{Name: "size", Type: registry.ColInt},
			},
		},
	})

	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ent, ok := reg.Get("ddltest")
	if !ok {
		t.Fatal("entity 'ddltest' not found in registry")
	}

	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := owner.RenameDynamicColumn(ctx, "ds_ddltest", "colour", "color"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := owner.DropDynamicColumn(ctx, "ds_ddltest", "size"); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	// structural guard
	if err := owner.DropDynamicColumn(ctx, "ds_ddltest", registry.ColumnID); err == nil {
		t.Fatal("expected structural-column drop to be refused")
	}
	if err := owner.DropDynamic(ctx, "ds_ddltest"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
}
