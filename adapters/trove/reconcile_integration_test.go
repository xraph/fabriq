//go:build integration

package trovestore_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/core/tenant"
)

// seedBlobObject inserts a catalog row so the reconciler's truth query
// (COUNT over blob_objects) sees a reference to hash. (Tasks here exercise the
// reconciler directly; the facade PutBlob path is covered by Task 6's e2e.)
func seedBlobObject(t *testing.T, pg *postgres.Adapter, tctx context.Context, id, hash string) {
	t.Helper()
	err := pg.TenantTxRaw(tctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(
			`INSERT INTO blob_objects (id, tenant_id, version, hash, size, content_type)
			 VALUES ($1, current_setting('app.tenant_id', true), 1, $2, 5, '')`,
			id, hash,
		).Exec(tctx)
		return err
	})
	if err != nil {
		t.Fatalf("seed blob_object: %v", err)
	}
}

func TestBlobReconcilerGCUnreferenced(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t)
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Store a byte but never create a blob_object → truth count is 0.
	hash, _, err := cs.Store(tctx, bytes.NewReader([]byte("orphan-ref")))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// grace=0 → immediately GC-eligible.
	rec := trovestore.NewBlobReconciler(cs, pg, 0)
	rep, err := rec.Reconcile(tctx, true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rep.GCCount != 1 {
		t.Fatalf("GCCount = %d, want 1", rep.GCCount)
	}
	// Byte and ledger row are gone.
	if _, err := cs.Retrieve(tctx, hash); err == nil {
		t.Fatal("byte should be GC'd, Retrieve succeeded")
	}
}

func TestBlobReconcilerGraceProtectsFresh(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t)
	tctx, _ := tenant.WithTenant(ctx, "acme")
	hash, _, err := cs.Store(tctx, bytes.NewReader([]byte("fresh")))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	rec := trovestore.NewBlobReconciler(cs, pg, time.Hour) // long grace
	rep, err := rec.Reconcile(tctx, true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rep.GCCount != 0 {
		t.Fatalf("GCCount = %d, want 0 (grace protects fresh entry)", rep.GCCount)
	}
	if _, err := cs.Retrieve(tctx, hash); err != nil {
		t.Fatalf("fresh byte should survive: %v", err)
	}
}

func TestBlobReconcilerRefCountRecompute(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t)
	tctx, _ := tenant.WithTenant(ctx, "acme")

	hash, _, err := cs.Store(tctx, bytes.NewReader([]byte("kept")))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	// One live reference exists. Store wrote ref_count=1, which already matches;
	// add two blob_object rows so truth=2 but ledger=1 → corrected up.
	seedBlobObject(t, pg, tctx, "bo-1", hash)
	seedBlobObject(t, pg, tctx, "bo-2", hash)

	rec := trovestore.NewBlobReconciler(cs, pg, 0)
	rep, err := rec.Reconcile(tctx, true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Truth is now 2 (two blob_objects), ledger ref_count was 1 → corrected.
	if rep.RefsCorrected != 1 {
		t.Fatalf("RefsCorrected = %d, want 1", rep.RefsCorrected)
	}
	if rep.GCCount != 0 {
		t.Fatalf("GCCount = %d, want 0 (still referenced)", rep.GCCount)
	}
}
