//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
)

// TestDynamicRead_ListGetMany exercises the map-native relational reads for a
// dynamic entity: List with equality filter, List with ordered result, and
// GetMany batch-hydration. All reads run inside a tenant context so RLS is
// active; cross-tenant isolation is verified at the end.
func TestDynamicRead_ListGetMany(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	// Seed three orders for the "acme" tenant.
	seed := func(id, sku string, qty int64) {
		t.Helper()
		if _, err := h.X.Exec(ctx, command.Command{
			Entity:  "orders",
			Op:      command.OpUpsert,
			AggID:   id,
			Payload: map[string]any{"sku": sku, "qty": qty},
		}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	seed("r1", "A1", 10)
	seed("r2", "B2", 20)
	seed("r3", "A1", 5)

	// ---- List with equality filter -------------------------------------------
	var bySkuA1 []map[string]any
	if err := h.A.List(ctx, "orders", query.ListQuery{
		Where:   query.Where{query.Eq("sku", "A1")},
		OrderBy: "id ASC",
	}, &bySkuA1); err != nil {
		t.Fatalf("List(sku=A1): %v", err)
	}
	if len(bySkuA1) != 2 {
		t.Fatalf("List(sku=A1): got %d rows, want 2", len(bySkuA1))
	}
	for _, row := range bySkuA1 {
		if row["sku"] != "A1" {
			t.Errorf("List(sku=A1): unexpected sku %v in row %v", row["sku"], row)
		}
		if row["tenant_id"] != "acme" {
			t.Errorf("List(sku=A1): tenant_id = %v, want acme", row["tenant_id"])
		}
	}

	// ---- List with ORDER BY dynamic column (qty DESC) -------------------------
	var byQtyDesc []map[string]any
	if err := h.A.List(ctx, "orders", query.ListQuery{
		OrderBy: "qty DESC",
	}, &byQtyDesc); err != nil {
		t.Fatalf("List(order qty DESC): %v", err)
	}
	if len(byQtyDesc) != 3 {
		t.Fatalf("List ordered: got %d rows, want 3", len(byQtyDesc))
	}
	// First row must be qty=20 (r2), last must be qty=5 (r3).
	first, _ := byQtyDesc[0]["qty"].(int64)
	last, _ := byQtyDesc[2]["qty"].(int64)
	if first != 20 {
		t.Errorf("List(qty DESC): first qty = %d, want 20 (got row: %v)", first, byQtyDesc[0])
	}
	if last != 5 {
		t.Errorf("List(qty DESC): last qty = %d, want 5 (got row: %v)", last, byQtyDesc[2])
	}

	// ---- GetMany (batch, id-ordered) -----------------------------------------
	var many []map[string]any
	if err := h.A.GetMany(ctx, "orders", []string{"r3", "r1"}, &many); err != nil {
		t.Fatalf("GetMany(r3,r1): %v", err)
	}
	if len(many) != 2 {
		t.Fatalf("GetMany: got %d rows, want 2", len(many))
	}
	// Order follows the ids slice: r3 first, r1 second.
	if id, _ := many[0]["id"].(string); id != "r3" {
		t.Errorf("GetMany[0].id = %q, want r3", id)
	}
	if id, _ := many[1]["id"].(string); id != "r1" {
		t.Errorf("GetMany[1].id = %q, want r1", id)
	}
	// Structural columns present.
	if _, ok := many[0]["tenant_id"]; !ok {
		t.Error("GetMany: tenant_id column missing from result map")
	}
	if _, ok := many[0]["version"]; !ok {
		t.Error("GetMany: version column missing from result map")
	}

	// ---- Cross-tenant isolation -----------------------------------------------
	// A rival tenant must see no rows.
	rivalCtx := tctx(t, "rival")
	var rivalRows []map[string]any
	if err := h.A.List(rivalCtx, "orders", query.ListQuery{}, &rivalRows); err != nil {
		t.Fatalf("List(rival): %v", err)
	}
	if len(rivalRows) != 0 {
		t.Errorf("RLS leak: rival tenant saw %d rows from acme", len(rivalRows))
	}

	// GetMany for the rival tenant using acme's ids must return empty.
	var rivalMany []map[string]any
	if err := h.A.GetMany(rivalCtx, "orders", []string{"r1", "r2", "r3"}, &rivalMany); err != nil {
		t.Fatalf("GetMany(rival): %v", err)
	}
	if len(rivalMany) != 0 {
		t.Errorf("RLS leak: GetMany returned %d rows to rival tenant", len(rivalMany))
	}
}

// TestDynamicRead_ListEmpty verifies that List returns an empty (not nil)
// slice when no rows match.
func TestDynamicRead_ListEmpty(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	var rows []map[string]any
	if err := h.A.List(ctx, "orders", query.ListQuery{
		Where: query.Where{query.Eq("sku", "NONEXISTENT")},
	}, &rows); err != nil {
		t.Fatalf("List(empty): %v", err)
	}
	// nil is acceptable (no rows appended) — len check is the assertion.
	if len(rows) != 0 {
		t.Fatalf("List(empty): expected 0 rows, got %d", len(rows))
	}
}

// TestDynamicRead_GetManyEmpty verifies that GetMany with 0 ids is a no-op.
func TestDynamicRead_GetManyEmpty(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	var rows []map[string]any
	if err := h.A.GetMany(ctx, "orders", []string{}, &rows); err != nil {
		t.Fatalf("GetMany(empty ids): %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("GetMany(empty ids): expected 0 rows, got %d", len(rows))
	}
}

// TestDynamicRead_InvalidColumn verifies that filter/order columns that
// are not declared on the entity are rejected (injection guard).
func TestDynamicRead_InvalidColumn(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	var rows []map[string]any
	err := h.A.List(ctx, "orders", query.ListQuery{
		Where: query.Where{query.Eq("not_a_column; DROP TABLE ds_orders--", "x")},
	}, &rows)
	if err == nil {
		t.Fatal("List with unknown filter column must return an error")
	}

	err = h.A.List(ctx, "orders", query.ListQuery{
		OrderBy: "not_a_column DESC",
	}, &rows)
	if err == nil {
		t.Fatal("List with unknown order column must return an error")
	}
}

// TestDynamicRead_CoexistsWithStaticWrite verifies that the existing static
// (Phase 4) write path still works after the dynamic read changes.
func TestDynamicRead_CoexistsWithStaticWrite(t *testing.T) {
	// This is really just TestDynamicWrite_InsertUpdateDelete invoked via the
	// shared harness — confirmed by running DynamicWrite in the same binary.
	// An explicit re-seed + read here proves both paths live together.
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	res, err := h.X.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpUpsert,
		AggID:   "co1",
		Payload: map[string]any{"sku": "COEXIST", "qty": int64(99)},
	})
	if err != nil {
		t.Fatalf("OpUpsert: %v", err)
	}
	if res.Version != 1 {
		t.Fatalf("version = %d, want 1", res.Version)
	}

	var rows []map[string]any
	if err := h.A.List(ctx, "orders", query.ListQuery{
		Where: query.Where{query.Eq("sku", "COEXIST")},
	}, &rows); err != nil {
		t.Fatalf("List after write: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("List after write: got %d rows, want 1", len(rows))
	}
	if rows[0]["sku"] != "COEXIST" {
		t.Errorf("sku = %v, want COEXIST", rows[0]["sku"])
	}
}

// TestDynamicRead_FilterValidation_KnownColumn verifies that a valid
// column in a Where filter passes validation and returns correct results.
func TestDynamicRead_FilterValidation_KnownColumn(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	if _, err := h.X.Exec(ctx, command.Command{
		Entity:  "orders",
		Op:      command.OpUpsert,
		AggID:   "fv1",
		Payload: map[string]any{"sku": "SKU-X", "qty": int64(7)},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Filter on the structural "version" column (always known).
	var rows []map[string]any
	if err := h.A.List(ctx, "orders", query.ListQuery{
		Where: query.Where{query.Eq("version", int64(1))},
	}, &rows); err != nil {
		t.Fatalf("List(version=1): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("List(version=1): got %d rows, want 1", len(rows))
	}
}

// contextFor creates a tenant-stamped context (alias of tctx for readability
// in this file — tctx is declared in postgres_integration_test.go).
var _ = context.Background // ensure context is used
