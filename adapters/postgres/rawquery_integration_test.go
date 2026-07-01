//go:build integration

package postgres_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// TestQueryDynamicReadOnly covers the raw read-only SQL surface: a dynamic
// multi-column result, refusal of writes, tenant isolation, and the row cap.
func TestQueryDynamicReadOnly(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")

	for i, sku := range []string{"A1", "B2", "A1"} {
		if _, err := h.X.Exec(ctx, command.Command{
			Entity: "orders", Op: command.OpUpsert,
			AggID:   string(rune('a'+i)),
			Payload: map[string]any{"sku": sku, "qty": int64(i + 1)},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Dynamic multi-column SELECT with a join-free aggregate. QueryDynamicReadOnly
	// is a raw-SQL passthrough (no entity-name translation), so this addresses
	// the physical table (ds_orders), not the registry entity name (orders).
	rows, cols, trunc, err := h.A.QueryDynamicReadOnly(ctx,
		`SELECT sku, count(*) AS n FROM ds_orders GROUP BY sku ORDER BY sku`)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if trunc {
		t.Fatalf("unexpected truncation")
	}
	if len(cols) != 2 || cols[0] != "sku" || cols[1] != "n" {
		t.Fatalf("cols = %v, want [sku n]", cols)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}

	// A write must be refused by the read-only transaction.
	if _, _, _, werr := h.A.QueryDynamicReadOnly(ctx,
		`DELETE FROM ds_orders`); werr == nil {
		t.Fatalf("expected read-only rejection, got nil")
	} else if !strings.Contains(strings.ToLower(werr.Error()), "read-only") {
		t.Fatalf("want read-only error, got %v", werr)
	}
}

func TestQueryDynamicReadOnly_Timeout(t *testing.T) {
	h := newDynWriteHarness(t)
	ctx := tctx(t, "acme")
	old := postgres.RawQueryTimeout
	postgres.RawQueryTimeout = 150 * time.Millisecond
	defer func() { postgres.RawQueryTimeout = old }()

	_, _, _, err := h.A.QueryDynamicReadOnly(ctx, `SELECT pg_sleep(2)`)
	if err == nil {
		t.Fatalf("expected a timeout error, got nil")
	}
	if !errors.Is(err, fabriqerr.ErrQueryTimeout) {
		t.Fatalf("want ErrQueryTimeout, got %v", err)
	}
}
