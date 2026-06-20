package trovestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// Report summarizes one tenant's reconcile pass.
type Report struct {
	RefsCorrected  int      // ledger rows whose ref_count disagreed with truth
	GCCount        int      // unreferenced+unpinned entries garbage-collected
	BytesFreed     int64    // bytes reclaimed by GC
	Broken         []string // referenced hashes whose bytes are missing
	OrphansDeleted int      // stored objects with no ledger row, removed
}

// BlobReconciler heals blob_cas drift and bounds byte storage for ONE tenant
// per Reconcile call (the caller stamps the tenant into ctx via
// tenant.WithTenant). It recomputes ref counts from the live blob_objects
// catalog (the command-authoritative truth), garbage-collects
// unreferenced/unpinned entries past a grace window, flags catalog rows whose
// bytes are missing, and deletes orphan bytes that no ledger row references.
//
// It lives in adapters/trove so it can reach CASStore's driver directly while
// keeping the trove import out of core/open.go (depguard).
type BlobReconciler struct {
	store *CASStore
	run   TenantTxRunner
	grace time.Duration
}

// NewBlobReconciler builds a reconciler over the per-tenant CAS store, running
// ledger SQL through run (tenant-stamped) and protecting entries younger than
// grace from GC.
func NewBlobReconciler(store *CASStore, run TenantTxRunner, grace time.Duration) *BlobReconciler {
	return &BlobReconciler{store: store, run: run, grace: grace}
}

type truthRow struct {
	Hash string `grove:"hash"`
	N    int64  `grove:"n"`
}

type ledgerRow struct {
	Hash      string    `grove:"hash"`
	Bucket    string    `grove:"bucket"`
	Key       string    `grove:"key"`
	Size      int64     `grove:"size"`
	RefCount  int64     `grove:"ref_count"`
	Pinned    bool      `grove:"pinned"`
	CreatedAt time.Time `grove:"created_at"`
}

// Reconcile runs all three checks for the tenant in ctx. With repair=false it
// reports drift without mutating anything (dry-run).
func (r *BlobReconciler) Reconcile(ctx context.Context, repair bool) (Report, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Report{}, err
	}

	truth, err := r.truthCounts(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("fabriq: blob reconcile: truth: %w", err)
	}
	ledger, err := r.ledgerRows(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("fabriq: blob reconcile: ledger: %w", err)
	}

	var rep Report
	ledgerHashes := make(map[string]struct{}, len(ledger))
	ledgerKeys := make(map[string]struct{}, len(ledger))
	for _, row := range ledger {
		ledgerHashes[row.Hash] = struct{}{}
		ledgerKeys[row.Key] = struct{}{}
		t := truth[row.Hash]

		// 1. Ref-count recompute from truth.
		if row.RefCount != t {
			rep.RefsCorrected++
			if repair && t > 0 {
				if err = r.setRefCount(ctx, row.Hash, t); err != nil {
					return rep, fmt.Errorf("fabriq: blob reconcile: correct ref_count %q: %w", row.Hash, err)
				}
			}
		}

		// 1b. GC unreferenced, unpinned entries past the grace window.
		if t == 0 && !row.Pinned && time.Since(row.CreatedAt) > r.grace {
			rep.GCCount++
			rep.BytesFreed += row.Size
			if repair {
				if err = r.deleteByte(ctx, row.Bucket, row.Key); err != nil {
					return rep, fmt.Errorf("fabriq: blob reconcile: gc byte %q: %w", row.Hash, err)
				}
				if err = r.deleteRow(ctx, row.Hash); err != nil {
					return rep, fmt.Errorf("fabriq: blob reconcile: gc row %q: %w", row.Hash, err)
				}
			}
			continue // a GC'd row is not also a broken-row candidate
		}

		// 2. Broken-row: a referenced hash whose bytes are missing.
		if t > 0 {
			ok, berr := r.byteExists(ctx, row.Bucket, row.Key)
			if berr != nil {
				return rep, fmt.Errorf("fabriq: blob reconcile: head %q: %w", row.Hash, berr)
			}
			if !ok {
				rep.Broken = append(rep.Broken, row.Hash)
			}
		}
	}

	// 2b. A referenced hash with NO ledger row at all is also broken.
	for hash, n := range truth {
		if n > 0 {
			if _, ok := ledgerHashes[hash]; !ok {
				rep.Broken = append(rep.Broken, hash)
			}
		}
	}

	// 3. Orphan-byte GC: objects in the tenant bucket with no ledger row.
	orphans, err := r.orphanKeys(ctx, tid, ledgerKeys)
	if err != nil {
		return rep, fmt.Errorf("fabriq: blob reconcile: orphan scan: %w", err)
	}
	bucket := bucketFor(r.store.base, tid)
	for _, key := range orphans {
		rep.OrphansDeleted++
		if repair {
			if err := r.deleteByte(ctx, bucket, key); err != nil {
				return rep, fmt.Errorf("fabriq: blob reconcile: gc orphan %q: %w", key, err)
			}
		}
	}
	return rep, nil
}

func (r *BlobReconciler) truthCounts(ctx context.Context) (map[string]int64, error) {
	var rows []truthRow
	err := r.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(`
			SELECT hash, COUNT(*) AS n FROM blob_objects GROUP BY hash
			UNION ALL
			SELECT summary_hash AS hash, COUNT(*) AS n FROM digest_nodes
				WHERE summary_hash <> '' GROUP BY summary_hash
		`).Scan(ctx, &rows)
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		// Use += not = so that a hash present in both blob_objects AND digest_nodes
		// produces the correct combined count rather than silently overwriting one
		// source with the other (which could drop a still-live reference to zero).
		out[row.Hash] += row.N
	}
	return out, nil
}

func (r *BlobReconciler) ledgerRows(ctx context.Context) ([]ledgerRow, error) {
	var rows []ledgerRow
	err := r.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(`SELECT hash, bucket, key, size, ref_count, pinned, created_at FROM blob_cas`).Scan(ctx, &rows)
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *BlobReconciler) setRefCount(ctx context.Context, hash string, n int64) error {
	return r.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(`UPDATE blob_cas SET ref_count = $1 WHERE hash = $2`, n, hash).Exec(ctx)
		return err
	})
}

func (r *BlobReconciler) deleteRow(ctx context.Context, hash string) error {
	return r.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(`DELETE FROM blob_cas WHERE hash = $1`, hash).Exec(ctx)
		return err
	})
}

// deleteByte removes a byte object; a not-found is treated as already-gone
// (GC is idempotent — a crash mid-GC re-converges next cycle).
func (r *BlobReconciler) deleteByte(ctx context.Context, bucket, key string) error {
	err := r.store.drv.Delete(ctx, bucket, key)
	if err != nil && !errors.Is(mapErr(err), fabriqerr.ErrNotFound) {
		return err
	}
	return nil
}

// byteExists heads a byte object, reporting absence as (false, nil).
func (r *BlobReconciler) byteExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := r.store.drv.Head(ctx, bucket, key)
	if err == nil {
		return true, nil
	}
	if errors.Is(mapErr(err), fabriqerr.ErrNotFound) {
		return false, nil
	}
	return false, err
}

// orphanKeys lists the tenant bucket and returns object keys that have no
// ledger row and whose last modification predates the grace window (so an
// in-flight Store is not collected before its ledger row commits).
func (r *BlobReconciler) orphanKeys(ctx context.Context, tenantID string, ledgerKeys map[string]struct{}) ([]string, error) {
	bucket := bucketFor(r.store.base, tenantID)
	it, err := r.store.drv.List(ctx, bucket)
	if err != nil {
		if errors.Is(mapErr(err), fabriqerr.ErrNotFound) {
			return nil, nil // bucket never created → no objects
		}
		return nil, err
	}
	var out []string
	for {
		info, ierr := it.Next(ctx)
		if errors.Is(ierr, io.EOF) {
			break
		}
		if ierr != nil {
			return nil, ierr
		}
		if _, ok := ledgerKeys[info.Key]; ok {
			continue
		}
		if time.Since(info.LastModified) <= r.grace {
			continue
		}
		out = append(out, info.Key)
	}
	return out, nil
}
