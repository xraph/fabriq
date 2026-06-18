package trovestore

import (
	"context"
	"io"

	trovecas "github.com/xraph/trove/cas"
	trovedriver "github.com/xraph/trove/driver"
)

// CASStore is a thin wrapper around *trovecas.CAS that keeps the trove/cas
// import confined to the adapters/ layer. open.go and core packages import
// only this wrapper type; they never import trove/cas directly.
type CASStore struct {
	c *trovecas.CAS
}

// NewCASStore constructs a CASStore backed by drv, indexed by idx, writing
// objects into bucket. The caller is responsible for ensuring the bucket
// already exists on the driver (e.g. via trove.Open + CreateBucket) before
// calling Store.
//
// Signature note: cas.New takes a driver.Driver (not *trove.Trove). Pass the
// raw driver obtained from memdriver.New() or troveAdapter.t.Driver() etc.
func NewCASStore(drv trovedriver.Driver, idx trovecas.Index, bucket string) *CASStore {
	return &CASStore{
		c: trovecas.New(drv, trovecas.WithIndex(idx), trovecas.WithBucket(bucket)),
	}
}

// Store writes content from r to the CAS. If identical content has been stored
// before, the Index's Put method is called with the existing entry, which
// increments ref_count via the ON CONFLICT clause in CASIndex.Put. The returned
// hash is stable across duplicate writes (same bytes → same hash).
func (s *CASStore) Store(ctx context.Context, r io.Reader) (string, *trovedriver.ObjectInfo, error) {
	return s.c.Store(ctx, r)
}
