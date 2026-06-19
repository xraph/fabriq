//go:build integration

package trovestore_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
	trovecas "github.com/xraph/trove/cas"

	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/tenant"
)

func TestBlobCASCreatedAtDefault(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t) // from cas_pertenant_integration_test.go
	_ = cs
	idx := trovestore.NewCASIndex(pg)

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(tctx, &trovecas.Entry{Hash: "h", Bucket: "c-acme", Key: "h", Size: 3, RefCount: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// created_at is populated by the DB default and is recent.
	type row struct {
		CreatedAt time.Time `grove:"created_at"`
	}
	var rows []row
	err = pg.TenantTxRaw(tctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(`SELECT created_at FROM blob_cas WHERE hash = $1`, "h").Scan(ctx, &rows)
	})
	if err != nil {
		t.Fatalf("select created_at: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].CreatedAt.IsZero() {
		t.Fatal("created_at is zero; DEFAULT now() not applied")
	}
	if time.Since(rows[0].CreatedAt) > time.Hour {
		t.Fatalf("created_at not recent: %v", rows[0].CreatedAt)
	}
}
