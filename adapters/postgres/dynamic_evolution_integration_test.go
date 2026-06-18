//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestDynamicEvolution_AdditiveSchemaChange verifies that EnsureDynamic is a
// create-or-evolve operation:
//
//  1. v1 descriptor creates ds_items with one domain column (name).
//  2. Seed a row so the table is non-empty before evolution.
//  3. v2 descriptor adds two nullable columns (note, qty) and a new index.
//     Calling EnsureDynamic with v2 must reconcile the table additively.
//  4. Assert via information_schema that note and qty now exist and the
//     pre-existing row has NULL for both new columns.
//  5. Assert via pg_indexes that the new index exists.
//  6. Assert that a new write including note/qty succeeds and reads back.
//  7. Idempotency: calling EnsureDynamic(v2) again must produce no error.
//  8. (Policy boundary) v3 adds a NOT-NULL-no-default column to the populated
//     table and asserts that EnsureDynamic returns a Postgres error — proving
//     fabriq does not silently bypass the unsafe-migration guard.
func TestDynamicEvolution_AdditiveSchemaChange(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	// ── v1 descriptor ──────────────────────────────────────────────────────────
	regV1 := registry.New()
	regV1.MustRegister(registry.EntitySpec{
		Name: "items",
		Schema: &registry.DynamicSchema{
			Table: "ds_items",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
			},
		},
	})

	ownerV1, err := postgres.Open(ctx, superDSN, regV1)
	if err != nil {
		t.Fatalf("postgres.Open (v1): %v", err)
	}
	t.Cleanup(func() { _ = ownerV1.Close() })

	// Apply fabriq migrations before creating the dynamic table.
	orch, err := migrations.NewOrchestrator(ownerV1.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	entV1, _ := regV1.Get("items")
	if err := ownerV1.EnsureDynamic(ctx, entV1); err != nil {
		t.Fatalf("EnsureDynamic (v1): %v", err)
	}

	// ── Seed a row so the table is non-empty before evolution ──────────────────
	pg := ownerV1.Driver()
	if _, err := pg.Exec(ctx,
		`INSERT INTO ds_items (id, tenant_id, version, name) VALUES ('seed1', 'acme', 1, 'Widget')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// ── v2 descriptor ──────────────────────────────────────────────────────────
	// Same table, adds note (nullable TEXT) + qty (nullable BIGINT) and one new index.
	regV2 := registry.New()
	regV2.MustRegister(registry.EntitySpec{
		Name: "items",
		Schema: &registry.DynamicSchema{
			Table: "ds_items",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "note", Type: registry.ColText}, // nullable — safe to add to existing rows
				{Name: "qty", Type: registry.ColInt},   // nullable — safe to add to existing rows
			},
			Indexes: []registry.DynamicIndex{
				{Name: "ds_items_qty_idx", Columns: []string{"qty"}},
			},
		},
	})

	ownerV2, err := postgres.Open(ctx, superDSN, regV2)
	if err != nil {
		t.Fatalf("postgres.Open (v2): %v", err)
	}
	t.Cleanup(func() { _ = ownerV2.Close() })

	entV2, _ := regV2.Get("items")
	if err := ownerV2.EnsureDynamic(ctx, entV2); err != nil {
		t.Fatalf("EnsureDynamic (v2, evolution): %v", err)
	}

	// ── Assert: new columns exist in information_schema ────────────────────────
	type colInfo struct {
		name     string
		dataType string
		nullable string // YES / NO
	}
	rows, err := pg.Query(ctx,
		`SELECT column_name, data_type, is_nullable
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'ds_items'
		 ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("information_schema query: %v", err)
	}
	defer rows.Close()

	colsByName := map[string]colInfo{}
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.name, &c.dataType, &c.nullable); err != nil {
			t.Fatal(err)
		}
		colsByName[c.name] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	for _, want := range []struct {
		col      string
		dataType string
		nullable string
	}{
		{"id", "text", "NO"},
		{"tenant_id", "text", "NO"},
		{"version", "bigint", "NO"},
		{"name", "text", "NO"},
		{"note", "text", "YES"},
		{"qty", "bigint", "YES"},
	} {
		got, ok := colsByName[want.col]
		if !ok {
			t.Errorf("column %q missing from ds_items after v2 evolution", want.col)
			continue
		}
		if got.dataType != want.dataType {
			t.Errorf("column %q: data_type = %q, want %q", want.col, got.dataType, want.dataType)
		}
		if got.nullable != want.nullable {
			t.Errorf("column %q: is_nullable = %q, want %q", want.col, got.nullable, want.nullable)
		}
	}

	// ── Assert: pre-existing row has NULL for the new columns ─────────────────
	var note *string
	var qty *int64
	if err := pg.QueryRow(ctx,
		`SELECT note, qty FROM ds_items WHERE id = 'seed1'`).Scan(&note, &qty); err != nil {
		t.Fatalf("scan seed row after evolution: %v", err)
	}
	if note != nil {
		t.Errorf("seed row note = %v, want NULL", *note)
	}
	if qty != nil {
		t.Errorf("seed row qty = %v, want NULL", *qty)
	}

	// ── Assert: new index exists ───────────────────────────────────────────────
	var idxName string
	if err := pg.QueryRow(ctx,
		`SELECT indexname FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename = 'ds_items' AND indexname = 'ds_items_qty_idx'`,
	).Scan(&idxName); err != nil {
		t.Fatalf("pg_indexes: ds_items_qty_idx not found: %v", err)
	}
	if idxName != "ds_items_qty_idx" {
		t.Errorf("index name = %q, want ds_items_qty_idx", idxName)
	}

	// ── Assert: new write with evolved columns succeeds ────────────────────────
	if _, err := pg.Exec(ctx,
		`INSERT INTO ds_items (id, tenant_id, version, name, note, qty)
		 VALUES ('new1', 'acme', 1, 'Gadget', 'nice', 42)`); err != nil {
		t.Fatalf("insert with new columns: %v", err)
	}
	var gotNote string
	var gotQty int64
	if err := pg.QueryRow(ctx,
		`SELECT note, qty FROM ds_items WHERE id = 'new1'`).Scan(&gotNote, &gotQty); err != nil {
		t.Fatalf("read back new row: %v", err)
	}
	if gotNote != "nice" {
		t.Errorf("note = %q, want nice", gotNote)
	}
	if gotQty != 42 {
		t.Errorf("qty = %d, want 42", gotQty)
	}

	// ── Idempotency: calling EnsureDynamic(v2) again must be a no-op ──────────
	if err := ownerV2.EnsureDynamic(ctx, entV2); err != nil {
		t.Fatalf("EnsureDynamic (v2, idempotency): %v", err)
	}

	// ── Policy boundary: NOT-NULL-no-default on a non-empty table must fail ────
	//
	// This asserts the additive-safety contract: fabriq does not bypass
	// Postgres's protection against unsafe NOT NULL migrations. The caller
	// must either supply a Default or add the column as nullable.
	regV3 := registry.New()
	regV3.MustRegister(registry.EntitySpec{
		Name: "items",
		Schema: &registry.DynamicSchema{
			Table: "ds_items",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "note", Type: registry.ColText},
				{Name: "qty", Type: registry.ColInt},
				// required_field is NOT NULL with no Default — Postgres will
				// reject the ALTER because the table already has rows.
				{Name: "required_field", Type: registry.ColText, NotNull: true},
			},
		},
	})
	ownerV3, err := postgres.Open(ctx, superDSN, regV3)
	if err != nil {
		t.Fatalf("postgres.Open (v3): %v", err)
	}
	t.Cleanup(func() { _ = ownerV3.Close() })

	entV3, _ := regV3.Get("items")
	if err := ownerV3.EnsureDynamic(ctx, entV3); err == nil {
		t.Error("EnsureDynamic (v3, NOT NULL no default on populated table): expected an error from Postgres, got nil")
	} else {
		t.Logf("EnsureDynamic (v3) correctly returned error: %v", err)
	}
}
