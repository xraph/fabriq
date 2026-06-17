package cache_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/cache"
)

// tinyCache is a minimal in-test Cache for exercising Typed[T] in isolation.
type tinyCache struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newTiny() *tinyCache { return &tinyCache{data: map[string][]byte{}} }

func (c *tinyCache) GetOrLoad(ctx context.Context, _ cache.Keyspace, key string,
	load func(context.Context) ([]byte, error)) ([]byte, error) {
	c.mu.Lock()
	if v, ok := c.data[key]; ok {
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()
	v, err := load(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.data[key] = v
	c.mu.Unlock()
	return v, nil
}
func (c *tinyCache) Get(_ context.Context, _ cache.Keyspace, key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[key]
	return v, ok, nil
}
func (c *tinyCache) Set(_ context.Context, _ cache.Keyspace, key string, val []byte) error {
	c.mu.Lock()
	c.data[key] = val
	c.mu.Unlock()
	return nil
}
func (c *tinyCache) Invalidate(_ context.Context, _ cache.Keyspace, keys ...string) error {
	c.mu.Lock()
	for _, k := range keys {
		delete(c.data, k)
	}
	c.mu.Unlock()
	return nil
}
func (c *tinyCache) InvalidateKeyspace(_ context.Context, _ cache.Keyspace) error { return nil }
func (c *tinyCache) Close() error                                                 { return nil }

func TestTypedGetOrLoad(t *testing.T) {
	type asset struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	ks := cache.Keyspace{Name: "asset.byid", Version: 1, Partition: cache.Tenant,
		Policy: cache.Policy{Mode: cache.EventEvict}}
	typed := cache.For[asset](newTiny(), ks)

	loads := 0
	load := func(context.Context) (asset, error) {
		loads++
		return asset{ID: "a1", Name: "Pump"}, nil
	}
	got, err := typed.GetOrLoad(context.Background(), "a1", load)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Pump" {
		t.Fatalf("got %+v", got)
	}
	// Second call is a hit: loader not invoked again.
	got2, err := typed.GetOrLoad(context.Background(), "a1", load)
	if err != nil {
		t.Fatal(err)
	}
	if got2 != got {
		t.Fatalf("hit mismatch: %+v vs %+v", got2, got)
	}
	if loads != 1 {
		t.Fatalf("loader called %d times, want 1", loads)
	}
}
