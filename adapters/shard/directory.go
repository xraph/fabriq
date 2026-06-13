package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// HashDirectory places tenants across a fixed set of shards by hashing the
// tenant id — stateless and deterministic, so every replica routes a tenant
// the same way without coordination. It is the simplest directory: it needs
// no catalog table, but changing the shard set re-places tenants (and so
// requires a data move). A catalog-backed directory with explicit placement
// (ADR 0007) is the drop-in upgrade when tenants must be moved individually.
func HashDirectory(ids ...string) Directory {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	return hashDirectory{ids: sorted}
}

type hashDirectory struct{ ids []string }

func (h hashDirectory) Shard(_ context.Context, tenantID string) (string, error) {
	if len(h.ids) == 0 {
		return "", fmt.Errorf("fabriq: hash directory has no shards")
	}
	sum := fnv.New32a()
	_, _ = sum.Write([]byte(tenantID))
	return h.ids[sum.Sum32()%uint32(len(h.ids))], nil
}

// Cached memoizes a directory's tenant→shard answers for ttl, so the hot
// path does not pay a lookup per operation. Placement is stable (a tenant's
// shard does not change without an explicit move), so a short TTL is purely
// a freshness bound for future catalog-backed directories; the hash
// directory is constant and caching it is free insurance.
func Cached(inner Directory, ttl time.Duration) Directory {
	return &cachedDirectory{inner: inner, ttl: ttl, cache: map[string]cacheEntry{}}
}

type cacheEntry struct {
	id  string
	exp time.Time
}

type cachedDirectory struct {
	inner Directory
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]cacheEntry
}

func (c *cachedDirectory) Shard(ctx context.Context, tenantID string) (string, error) {
	c.mu.Lock()
	if e, ok := c.cache[tenantID]; ok && time.Now().Before(e.exp) {
		c.mu.Unlock()
		return e.id, nil
	}
	c.mu.Unlock()

	id, err := c.inner.Shard(ctx, tenantID)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.cache[tenantID] = cacheEntry{id: id, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return id, nil
}
