package fabriqtest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/xraph/fabriq/core/blob"
)

// FakeCAS is an in-memory content-addressed store for hermetic tests. It
// implements blob.CAS, dedups by sha256, and is tenant-agnostic.
type FakeCAS struct {
	mu   sync.RWMutex
	data map[string][]byte
}

var _ blob.CAS = (*FakeCAS)(nil)

// NewFakeCAS builds an empty FakeCAS.
func NewFakeCAS() *FakeCAS { return &FakeCAS{data: map[string][]byte{}} }

// Store writes content-addressed bytes and returns the sha256 hex hash.
func (c *FakeCAS) Store(_ context.Context, r io.Reader) (string, int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(b)
	h := hex.EncodeToString(sum[:])
	c.mu.Lock()
	if _, ok := c.data[h]; !ok {
		c.data[h] = b
	}
	c.mu.Unlock()
	return h, int64(len(b)), nil
}

// Retrieve returns the bytes for a hash.
func (c *FakeCAS) Retrieve(_ context.Context, hash string) (io.ReadCloser, error) {
	c.mu.RLock()
	b, ok := c.data[hash]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("fabriqtest: cas hash %q not found", hash)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// Has reports whether a hash is present.
func (c *FakeCAS) Has(hash string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.data[hash]
	return ok
}

// Len returns the number of distinct stored blobs.
func (c *FakeCAS) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}
