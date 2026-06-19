package trovestore

import (
	"context"
	"fmt"
	"io"
	"sync"

	trovecas "github.com/xraph/trove/cas"
	trovedriver "github.com/xraph/trove/driver"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/tenant"
)

// CASStore is a per-tenant content-addressable store. Each tenant's bytes live
// in their OWN bucket (bucketFor(base, tenantID)), so per-tenant GC of one
// tenant's unreferenced object can never touch another tenant's bytes — even
// when both stored identical content (same hash). The trove/cas import stays
// confined to this adapters/ package; open.go and core import only this type.
type CASStore struct {
	drv  trovedriver.Driver
	idx  trovecas.Index
	base string

	mu        sync.Mutex
	perTenant map[string]*trovecas.CAS
}

// Compile-time assertion: CASStore satisfies the fabriq blob.CAS port.
var _ blob.CAS = (*CASStore)(nil)

// NewCASStore constructs a per-tenant CASStore over drv, indexing entries in
// idx and deriving each tenant's bucket from base. The signature is unchanged
// from Phase 3 (open.go passes the same three arguments); the third argument
// is now the bucket BASE used for per-tenant derivation, not a shared bucket.
//
// Signature note: cas.New takes a driver.Driver (not *trove.Trove). Pass the
// raw driver obtained from memdriver.New() or troveAdapter.Driver().
func NewCASStore(drv trovedriver.Driver, idx trovecas.Index, bucket string) *CASStore {
	return &CASStore{
		drv:       drv,
		idx:       idx,
		base:      bucket,
		perTenant: make(map[string]*trovecas.CAS),
	}
}

// bucketFor derives a tenant's private bucket name from the configured base.
// Two tenants therefore never share a bucket: identical content is stored as
// separate objects and per-tenant GC is isolated. (Local/mem drivers — the
// configured backends — accept this naming; S3-style sanitization is a noted
// future concern, see the Phase 4 spec open questions.)
func bucketFor(base, tenantID string) string {
	return base + "-" + tenantID
}

// forTenant returns the *cas.CAS bound to the calling tenant's private bucket,
// building and caching it once per tenant and ensuring the bucket exists.
func (s *CASStore) forTenant(ctx context.Context, tenantID string) (*trovecas.CAS, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.perTenant[tenantID]; ok {
		return c, nil
	}
	bucket := bucketFor(s.base, tenantID)
	// CreateBucket is idempotent across the local/mem drivers; a pre-existing
	// bucket is not an error we act on (Open uses the same ignore pattern).
	_ = s.drv.CreateBucket(ctx, bucket)
	c := trovecas.New(s.drv, trovecas.WithIndex(s.idx), trovecas.WithBucket(bucket))
	s.perTenant[tenantID] = c
	return c, nil
}

// Store writes content from r to the calling tenant's CAS bucket. Identical
// content stored before by THIS tenant increments ref_count (ON CONFLICT in
// CASIndex.Put); the returned hash is stable across duplicate writes. The
// trove *driver.ObjectInfo is unwrapped internally; callers receive only
// fabriq types.
func (s *CASStore) Store(ctx context.Context, r io.Reader) (string, int64, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return "", 0, err
	}
	c, err := s.forTenant(ctx, tid)
	if err != nil {
		return "", 0, err
	}
	hash, info, err := c.Store(ctx, r)
	if err != nil {
		return "", 0, err
	}
	var size int64
	if info != nil {
		size = info.Size
	}
	return hash, size, nil
}

// Retrieve returns the bytes stored under hash for the calling tenant as an
// io.ReadCloser. The caller is responsible for closing the reader. Returns an
// error if the hash is not found in this tenant's bucket.
func (s *CASStore) Retrieve(ctx context.Context, hash string) (io.ReadCloser, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.forTenant(ctx, tid)
	if err != nil {
		return nil, err
	}
	obj, err := c.Retrieve(ctx, hash)
	if err != nil {
		return nil, err
	}
	// *driver.ObjectReader embeds io.ReadCloser directly. Guard against a
	// driver returning (nil, nil) — avoids a latent nil-deref panic.
	if obj == nil || obj.ReadCloser == nil {
		return nil, fmt.Errorf("fabriq: trove cas: retrieve %q: driver returned nil reader", hash)
	}
	return obj.ReadCloser, nil
}
