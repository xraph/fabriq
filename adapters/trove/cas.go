package trovestore

import (
	"context"
	"fmt"
	"io"

	trovecas "github.com/xraph/trove/cas"
	trovedriver "github.com/xraph/trove/driver"

	"github.com/xraph/fabriq/core/blob"
)

// CASStore is a thin wrapper around *trovecas.CAS that keeps the trove/cas
// import confined to the adapters/ layer. open.go and core packages import
// only this wrapper type; they never import trove/cas directly.
type CASStore struct {
	c *trovecas.CAS
}

// Compile-time assertion: CASStore satisfies the fabriq blob.CAS port.
var _ blob.CAS = (*CASStore)(nil)

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
// hash is stable across duplicate writes (same bytes → same hash). The trove
// *driver.ObjectInfo is unwrapped internally; callers receive only fabriq types.
func (s *CASStore) Store(ctx context.Context, r io.Reader) (string, int64, error) {
	hash, info, err := s.c.Store(ctx, r)
	if err != nil {
		return "", 0, err
	}
	var size int64
	if info != nil {
		size = info.Size
	}
	return hash, size, nil
}

// Retrieve returns the bytes stored under hash as an io.ReadCloser. The caller
// is responsible for closing the reader. Returns an error if the hash is not found.
func (s *CASStore) Retrieve(ctx context.Context, hash string) (io.ReadCloser, error) {
	obj, err := s.c.Retrieve(ctx, hash)
	if err != nil {
		return nil, err
	}
	// *driver.ObjectReader embeds io.ReadCloser directly (confirmed via go doc).
	// Guard against a driver returning (nil, nil) — avoids a latent nil-deref panic.
	if obj == nil || obj.ReadCloser == nil {
		return nil, fmt.Errorf("fabriq: trove cas: retrieve %q: driver returned nil reader", hash)
	}
	return obj.ReadCloser, nil
}
