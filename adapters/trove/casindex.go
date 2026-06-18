package trovestore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/xraph/grove/drivers/pgdriver"
	trovecas "github.com/xraph/trove/cas"
)

// TenantTxRunner is a seam that opens a tenant-stamped raw SQL transaction.
// *postgres.Adapter satisfies this interface.
type TenantTxRunner interface {
	TenantTxRaw(ctx context.Context, fn func(tx *pgdriver.PgTx) error) error
}

// CASIndex is a per-tenant cas.Index backed by the blob_cas table.
// Every method runs inside TenantTxRaw so FORCE RLS isolates rows to the
// calling tenant; no tenant predicate is needed in the WHERE clauses.
type CASIndex struct{ run TenantTxRunner }

// Compile-time assertion: CASIndex satisfies the cas.Index interface.
var _ trovecas.Index = (*CASIndex)(nil)

// NewCASIndex returns a CASIndex that runs queries through run.
func NewCASIndex(run TenantTxRunner) *CASIndex { return &CASIndex{run: run} }

// casRow is the internal scan target for SELECT rows from blob_cas.
type casRow struct {
	Hash     string `grove:"hash"`
	Bucket   string `grove:"bucket"`
	Key      string `grove:"key"`
	Size     int64  `grove:"size"`
	RefCount int    `grove:"ref_count"`
	Pinned   bool   `grove:"pinned"`
}

func rowToEntry(r casRow) *trovecas.Entry {
	return &trovecas.Entry{
		Hash:     r.Hash,
		Bucket:   r.Bucket,
		Key:      r.Key,
		Size:     r.Size,
		RefCount: r.RefCount,
		Pinned:   r.Pinned,
	}
}

// Get returns the entry for hash, or trovecas.ErrNotFound if absent.
func (c *CASIndex) Get(ctx context.Context, hash string) (*trovecas.Entry, error) {
	var rows []casRow
	err := c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(
			`SELECT hash, bucket, key, size, ref_count, pinned FROM blob_cas WHERE hash = $1`,
			hash,
		).Scan(ctx, &rows)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, trovecas.ErrNotFound
		}
		return nil, err
	}
	if len(rows) == 0 {
		return nil, trovecas.ErrNotFound
	}
	return rowToEntry(rows[0]), nil
}

// Put stores or upserts an entry. If the hash already exists for this tenant,
// the stored ref_count is incremented (per cas.Index contract).
func (c *CASIndex) Put(ctx context.Context, e *trovecas.Entry) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(
			`INSERT INTO blob_cas (id, tenant_id, hash, bucket, key, size, ref_count, pinned)
			 VALUES (gen_random_uuid()::text, current_setting('app.tenant_id', true),
			         $1, $2, $3, $4, $5, $6)
			 ON CONFLICT (tenant_id, hash) DO UPDATE
			   SET bucket    = EXCLUDED.bucket,
			       key       = EXCLUDED.key,
			       size      = EXCLUDED.size,
			       ref_count = blob_cas.ref_count + 1`,
			e.Hash, e.Bucket, e.Key, e.Size, e.RefCount, e.Pinned,
		).Exec(ctx)
		return err
	})
}

// Delete removes the entry for hash from this tenant's namespace.
// Returns trovecas.ErrNotFound when no row matched (matches trove's reference MemoryIndex semantics).
func (c *CASIndex) Delete(ctx context.Context, hash string) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		res, err := tx.NewRaw(
			`DELETE FROM blob_cas WHERE hash = $1`, hash,
		).Exec(ctx)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return trovecas.ErrNotFound
		}
		return nil
	})
}

// IncrementRef adds 1 to the ref_count for hash.
// Returns trovecas.ErrNotFound when no row matched (matches trove's reference MemoryIndex semantics).
func (c *CASIndex) IncrementRef(ctx context.Context, hash string) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		res, err := tx.NewRaw(
			`UPDATE blob_cas SET ref_count = ref_count + 1 WHERE hash = $1`, hash,
		).Exec(ctx)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return trovecas.ErrNotFound
		}
		return nil
	})
}

// DecrementRef subtracts 1 from the ref_count for hash.
// Returns trovecas.ErrNotFound when no row matched (matches trove's reference MemoryIndex semantics).
func (c *CASIndex) DecrementRef(ctx context.Context, hash string) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		res, err := tx.NewRaw(
			`UPDATE blob_cas SET ref_count = ref_count - 1 WHERE hash = $1`, hash,
		).Exec(ctx)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return trovecas.ErrNotFound
		}
		return nil
	})
}

// Pin marks the entry as pinned, preventing garbage collection.
// Returns trovecas.ErrNotFound when no row matched (matches trove's reference MemoryIndex semantics).
func (c *CASIndex) Pin(ctx context.Context, hash string) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		res, err := tx.NewRaw(
			`UPDATE blob_cas SET pinned = true WHERE hash = $1`, hash,
		).Exec(ctx)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return trovecas.ErrNotFound
		}
		return nil
	})
}

// Unpin clears the pinned flag, making the entry eligible for GC.
// Returns trovecas.ErrNotFound when no row matched (matches trove's reference MemoryIndex semantics).
func (c *CASIndex) Unpin(ctx context.Context, hash string) error {
	return c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		res, err := tx.NewRaw(
			`UPDATE blob_cas SET pinned = false WHERE hash = $1`, hash,
		).Exec(ctx)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return trovecas.ErrNotFound
		}
		return nil
	})
}

// ListUnpinned returns all entries for this tenant that have ref_count = 0
// and pinned = false (eligible for garbage collection). RLS already scopes
// the query to the calling tenant — no explicit tenant predicate needed.
func (c *CASIndex) ListUnpinned(ctx context.Context) ([]*trovecas.Entry, error) {
	var rows []casRow
	err := c.run.TenantTxRaw(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(
			`SELECT hash, bucket, key, size, ref_count, pinned
			   FROM blob_cas
			  WHERE ref_count = 0 AND pinned = false`,
		).Scan(ctx, &rows)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]*trovecas.Entry, len(rows))
	for i, r := range rows {
		out[i] = rowToEntry(r)
	}
	return out, nil
}
