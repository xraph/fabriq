//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// dynWriteHarness boots a Postgres container, applies fabriq migrations,
// creates the dynamic ds_orders table via EnsureDynamic (as superuser),
// then provisions the app role and builds the executor against it.
type dynWriteHarness struct {
	superPG *postgres.Adapter // schema owner — used for DDL and row verification
	A       *postgres.Adapter // app role — RLS-constrained
	X       *command.Executor
	Reg     *registry.Registry
}

func newDynWriteHarness(t testing.TB) *dynWriteHarness {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

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
		},
	})

	// Open as schema owner for migrations + DDL.
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	// Apply fabriq migrations (outbox, projection state, static table RLS, etc.).
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Create the dynamic table as the schema owner (must come before app role so
	// DEFAULT PRIVILEGES grant the new table to fabriq_app).
	ent, ok := reg.Get("orders")
	if !ok {
		t.Fatal("entity 'orders' not found")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}

	// Provision the app role (after migrations so DEFAULT PRIVILEGES apply,
	// and after EnsureDynamic so the new table is covered by GRANT).
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	x, err := command.NewExecutor(reg, a)
	if err != nil {
		t.Fatal(err)
	}
	return &dynWriteHarness{superPG: owner, A: a, X: x, Reg: reg}
}

// TestDynamicWrite_InsertUpdateDelete exercises the full OpUpsert→OpUpsert→OpDelete
// lifecycle for a dynamic entity, verifying:
//   - The row lands in ds_orders with the correct scalar values.
//   - An UPDATE changes values in-place (COUNT stays 1).
//   - A DELETE removes the row (COUNT becomes 0).
//   - version increments correctly across operations.
func TestDynamicWrite_InsertUpdateDelete(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")
	superCtx := context.Background()

	// ---- CREATE (OpUpsert on a new aggID resolves to OpCreate inside executor) ---
	res, err := h.X.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpUpsert,
		AggID:   "o1",
		Payload: map[string]any{"sku": "A1", "qty": int64(3)},
	})
	if err != nil {
		t.Fatalf("OpUpsert (create): %v", err)
	}
	if res.Version != 1 {
		t.Fatalf("version after create = %d, want 1", res.Version)
	}

	// Verify row via superuser (bypasses RLS so the scan always works).
	type orderRow struct {
		ID       string
		TenantID string
		Version  int64
		SKU      string
		QTY      *int64
	}
	readRow := func(label string) orderRow {
		t.Helper()
		rows, err := h.superPG.Driver().Query(superCtx,
			`SELECT id, tenant_id, version, sku, qty FROM ds_orders WHERE id = 'o1'`)
		if err != nil {
			t.Fatalf("%s: query ds_orders: %v", label, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s: row not found", label)
		}
		var r orderRow
		if err := rows.Scan(&r.ID, &r.TenantID, &r.Version, &r.SKU, &r.QTY); err != nil {
			t.Fatalf("%s: scan: %v", label, err)
		}
		return r
	}
	countRows := func(label string) int {
		t.Helper()
		var n int
		if err := h.superPG.Driver().QueryRow(superCtx,
			`SELECT COUNT(*) FROM ds_orders WHERE id = 'o1'`).Scan(&n); err != nil {
			t.Fatalf("%s: count: %v", label, err)
		}
		return n
	}

	r := readRow("after create")
	if r.ID != "o1" {
		t.Errorf("id = %q, want %q", r.ID, "o1")
	}
	if r.TenantID != "acme" {
		t.Errorf("tenant_id = %q, want %q", r.TenantID, "acme")
	}
	if r.Version != 1 {
		t.Errorf("version = %d, want 1", r.Version)
	}
	if r.SKU != "A1" {
		t.Errorf("sku = %q, want %q", r.SKU, "A1")
	}
	if r.QTY == nil || *r.QTY != 3 {
		t.Errorf("qty = %v, want 3", r.QTY)
	}

	// ---- UPDATE (OpUpsert on an existing aggID resolves to OpUpdate) ----
	res2, err := h.X.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpUpsert,
		AggID:   "o1",
		Payload: map[string]any{"sku": "A2", "qty": int64(5)},
	})
	if err != nil {
		t.Fatalf("OpUpsert (update): %v", err)
	}
	if res2.Version != 2 {
		t.Fatalf("version after update = %d, want 2", res2.Version)
	}

	r2 := readRow("after update")
	if r2.SKU != "A2" {
		t.Errorf("sku after update = %q, want %q", r2.SKU, "A2")
	}
	if r2.QTY == nil || *r2.QTY != 5 {
		t.Errorf("qty after update = %v, want 5", r2.QTY)
	}
	if r2.Version != 2 {
		t.Errorf("version after update = %d, want 2", r2.Version)
	}
	// Must still be exactly one row — UPDATE not a new INSERT.
	if n := countRows("after update"); n != 1 {
		t.Errorf("row count after update = %d, want 1", n)
	}

	// ---- DELETE ----
	if _, err := h.X.Exec(ctx, command.Command{
		Entity: "orders",
		Op:     command.OpDelete,
		AggID:  "o1",
	}); err != nil {
		t.Fatalf("OpDelete: %v", err)
	}
	if n := countRows("after delete"); n != 0 {
		t.Errorf("row count after delete = %d, want 0", n)
	}
}
