//go:build integration

package postgres_test

import (
	"context"
	"testing"
)

// TestTableColumns_DynamicEntity verifies that TableColumns reports both the
// structural columns (id, tenant_id, version) and the domain columns declared
// on a dynamic entity's schema, reading from information_schema against the
// physical ds_orders table created by EnsureDynamic.
func TestTableColumns_DynamicEntity(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := context.Background()

	cols, err := h.superPG.TableColumns(ctx, "ds_orders")
	if err != nil {
		t.Fatalf("TableColumns(ds_orders): %v", err)
	}
	if len(cols) == 0 {
		t.Fatal("TableColumns(ds_orders): got 0 columns, want structural + domain columns")
	}

	byName := make(map[string]bool, len(cols))
	for _, c := range cols {
		byName[c.Name] = true
	}

	for _, want := range []string{"id", "tenant_id", "version", "sku", "qty", "meta"} {
		if !byName[want] {
			t.Errorf("TableColumns(ds_orders): missing column %q, got %v", want, cols)
		}
	}
}

// TestTableColumns_MissingTable verifies that querying a table that does not
// exist returns an empty slice and no error (information_schema simply has no
// matching rows).
func TestTableColumns_MissingTable(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := context.Background()

	cols, err := h.superPG.TableColumns(ctx, "table_that_does_not_exist")
	if err != nil {
		t.Fatalf("TableColumns(missing table): unexpected error: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("TableColumns(missing table): got %d columns, want 0", len(cols))
	}
}

// TestTableColumns_InvalidIdentifier verifies the ddlValid guard rejects
// non-identifier input before it ever reaches SQL.
func TestTableColumns_InvalidIdentifier(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := context.Background()

	if _, err := h.superPG.TableColumns(ctx, "not a table; DROP TABLE ds_orders--"); err == nil {
		t.Fatal("TableColumns with an invalid identifier must return an error")
	}
}
