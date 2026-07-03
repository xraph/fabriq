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
// (COUNT over fabriq_blob_objects) sees a reference to hash. (Tasks here exercise the
// reconciler directly; the facade PutBlob path is covered by Task 6's e2e.)
func seedBlobObject(t *testing.T, pg *postgres.Adapter, tctx context.Context, id, hash string) {
	t.Helper()
	err := pg.TenantTxRaw(tctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(
			`INSERT INTO fabriq_blob_objects (id, tenant_id, version, hash, size, content_type)
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
	// Truth is now 2 (two fabriq_blob_objects), ledger ref_count was 1 → corrected.
	if rep.RefsCorrected != 1 {
		t.Fatalf("RefsCorrected = %d, want 1", rep.RefsCorrected)
	}
	if rep.GCCount != 0 {
		t.Fatalf("GCCount = %d, want 0 (still referenced)", rep.GCCount)
	}
}

// seedDigestNode inserts a fabriq_digest_nodes row whose summary_hash points at the
// given CAS hash. This simulates the context-distillation path that writes a
// summary blob and records its hash in the digest tree — without creating a
// fabriq_blob_objects row, so the reconciler's old fabriq_blob_objects-only truth query would
// see count=0 and GC the blob.
func seedDigestNode(t *testing.T, pg *postgres.Adapter, tctx context.Context, id, summaryHash string) {
	t.Helper()
	err := pg.TenantTxRaw(tctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(
			`INSERT INTO fabriq_digest_nodes
				(id, tenant_id, version, level, kind, summary_hash)
			 VALUES ($1, current_setting('app.tenant_id', true), 1, 0, 'entity', $2)`,
			id, summaryHash,
		).Exec(tctx)
		return err
	})
	if err != nil {
		t.Fatalf("seed digest_node: %v", err)
	}
}

// TestBlobReconcilerDigestNodeIsGCRoot verifies that a CAS blob referenced only
// via fabriq_digest_nodes.summary_hash (no fabriq_blob_objects row) is NOT garbage-collected.
// This guards the UNION ALL + += fix in truthCounts: without it, the
// fabriq_digest_nodes root would be invisible to the GC and the summary blob would be
// deleted even while a DigestNode still points at it.
func TestBlobReconcilerDigestNodeIsGCRoot(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t)
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Store the summary blob. CAS creates a fabriq_blob_cas ledger row but no
	// fabriq_blob_objects row (that's the facade's job, not done here on purpose).
	hash, _, err := cs.Store(tctx, bytes.NewReader([]byte("summary-content")))
	if err != nil {
		t.Fatalf("Store summary blob: %v", err)
	}

	// Insert a digest_node that references the blob via summary_hash only.
	// There is intentionally NO fabriq_blob_objects row for this hash.
	seedDigestNode(t, pg, tctx, "dn-root-1", hash)

	// grace=0 → would immediately GC if not treated as a root.
	rec := trovestore.NewBlobReconciler(cs, pg, 0)
	rep, err := rec.Reconcile(tctx, true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The blob must survive — fabriq_digest_nodes root keeps it alive.
	if rep.GCCount != 0 {
		t.Fatalf("GCCount = %d, want 0: digest_node summary_hash must be a GC root", rep.GCCount)
	}

	// Byte is still retrievable.
	rc, err := cs.Retrieve(tctx, hash)
	if err != nil {
		t.Fatalf("summary blob should survive GC but Retrieve failed: %v", err)
	}
	_ = rc.Close()
}

// TestBlobReconcilerDigestAndBlobObjectSameHash verifies that when a hash
// appears in BOTH fabriq_blob_objects AND fabriq_digest_nodes, the += accumulation yields the
// correct combined count — not just one or the other (overwrite regression).
func TestBlobReconcilerDigestAndBlobObjectSameHash(t *testing.T) {
	ctx := context.Background()
	pg, cs := migrateAppCAS(t)
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Store the blob (1 CAS row with ref_count=1 from Store).
	hash, _, err := cs.Store(tctx, bytes.NewReader([]byte("shared")))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Add one fabriq_blob_objects row AND one digest_node referencing the same hash.
	seedBlobObject(t, pg, tctx, "bo-shared", hash)
	seedDigestNode(t, pg, tctx, "dn-shared", hash)

	// grace=0, so if truth count is 0 it would be GC'd.
	rec := trovestore.NewBlobReconciler(cs, pg, 0)
	rep, err := rec.Reconcile(tctx, true)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Combined truth count is 2 (1 from fabriq_blob_objects + 1 from fabriq_digest_nodes),
	// ledger ref_count was 1 → corrected to 2.
	if rep.GCCount != 0 {
		t.Fatalf("GCCount = %d, want 0: hash referenced by both sources must not be GC'd", rep.GCCount)
	}
	// ref_count in ledger was 1 (set by Store), truth is now 2 → corrected.
	if rep.RefsCorrected != 1 {
		t.Fatalf("RefsCorrected = %d, want 1 (fabriq_blob_objects+fabriq_digest_nodes combined count)", rep.RefsCorrected)
	}
}
