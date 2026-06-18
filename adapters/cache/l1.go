package cache

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xraph/fabriq/cachequery"
	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
)

// L1Cache is a per-node in-process cache in front of an inner cache.Cache (the
// shared Redis L2). It implements cache.Cache so it is transparent to all
// consumers. Query-keyspace entries are kept coherent via a LOCAL generation
// map (no per-access L2 read); row-keyspace entries evict per-id (preserving
// the P3a sibling-warmth invariant). The writing node clears its own L1 through
// the normal Invalidate*/Set path; other nodes via EvictLocal (driven by the
// broadcast event tailer).
type L1Cache struct {
	inner corecache.Cache
	store *l1Store
	reg   *registry.Registry

	mu  sync.Mutex
	gen map[string]int64 // "{genSource}|{partition}" -> local generation
}

// NewL1 wraps inner with a per-node L1 of at most size entries, each TTL'd.
func NewL1(inner corecache.Cache, reg *registry.Registry, size int, ttl time.Duration) *L1Cache {
	return &L1Cache{inner: inner, store: newL1Store(size, ttl), reg: reg, gen: map[string]int64{}}
}

var _ corecache.Cache = (*L1Cache)(nil)

func genSource(ks corecache.Keyspace) string {
	if ks.Entity != "" {
		return ks.Entity
	}
	return ks.Name
}

func (l *L1Cache) localGen(src, part string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gen[src+"|"+part]
}

func (l *L1Cache) bumpGen(src, part string) {
	l.mu.Lock()
	l.gen[src+"|"+part]++
	l.mu.Unlock()
}

// l1key composes the local L1 key for (ks, partition, key) at the current local
// generation. NOT the L2 generation — that is internal to the inner adapter.
func (l *L1Cache) l1key(ks corecache.Keyspace, part, key string) string {
	g := l.localGen(genSource(ks), part)
	return ks.Name + "|v" + strconv.Itoa(ks.Version) + "|g" + strconv.FormatInt(g, 10) + "|" + part + "|" + key
}

// Get returns the cached value from L1 on a hit; on an L1 miss it falls
// through to the inner L2. When the inner returns a hit the value is
// populated into L1 (write-on-read) so subsequent Gets are served locally.
// This preserves the cache hierarchy: a cold L1 (fresh node, after eviction,
// or after restart) still benefits from the shared L2 rather than forcing a
// DB reload on every Get.
func (l *L1Cache) Get(ctx context.Context, ks corecache.Keyspace, key string) (val []byte, ok bool, err error) {
	var part string
	part, err = ks.Partition.Resolve(ctx)
	if err != nil {
		return nil, false, err
	}
	lk := l.l1key(ks, part, key)
	if val, ok = l.store.get(lk); ok {
		return val, true, nil
	}
	val, ok, err = l.inner.Get(ctx, ks, key)
	if err != nil {
		return nil, false, err
	}
	if ok {
		l.store.put(lk, val)
	}
	return val, ok, nil
}

// Set stores a value in the inner L2 and populates L1.
func (l *L1Cache) Set(ctx context.Context, ks corecache.Keyspace, key string, val []byte) error {
	if err := l.inner.Set(ctx, ks, key, val); err != nil {
		return err
	}
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	l.store.put(l.l1key(ks, part, key), val)
	return nil
}

// GetOrLoad returns the cached value for key, or calls load exactly once on a
// miss, stores the result, and returns it.
func (l *L1Cache) GetOrLoad(ctx context.Context, ks corecache.Keyspace, key string,
	load func(context.Context) ([]byte, error)) ([]byte, error) {
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	lk := l.l1key(ks, part, key)
	if v, ok := l.store.get(lk); ok {
		return v, nil
	}
	v, err := l.inner.GetOrLoad(ctx, ks, key, load)
	if err != nil {
		return nil, err
	}
	l.store.put(lk, v)
	return v, nil
}

// Invalidate removes specific keys from the keyspace (targeted per-id eviction).
// Delegates to inner and removes the matching L1 entries.
func (l *L1Cache) Invalidate(ctx context.Context, ks corecache.Keyspace, keys ...string) error {
	if err := l.inner.Invalidate(ctx, ks, keys...); err != nil {
		return err
	}
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		l.store.del(l.l1key(ks, part, k))
	}
	return nil
}

// InvalidateKeyspace bumps the keyspace generation in inner and orphans the
// corresponding local generation so all L1 entries for this keyspace+partition
// are abandoned without mass deletion.
func (l *L1Cache) InvalidateKeyspace(ctx context.Context, ks corecache.Keyspace) error {
	if err := l.inner.InvalidateKeyspace(ctx, ks); err != nil {
		return err
	}
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	l.bumpGen(genSource(ks), part)
	return nil
}

// InvalidateEntity bumps the entity generation in inner (L2) and locally, so
// every query keyspace (Entity == entity) is orphaned in L1 across all
// partitions. Row keyspaces (no Entity) are NOT bumped here; they evict per-id.
func (l *L1Cache) InvalidateEntity(ctx context.Context, entity string) error {
	if err := l.inner.InvalidateEntity(ctx, entity); err != nil {
		return err
	}
	for _, part := range entityPartitions(ctx) {
		l.bumpGen(entity, part)
	}
	return nil
}

// EvictLocal clears this node's L1 for a committed change to (entity, ids)
// observed via the event stream. It is LOCAL-ONLY (never touches the inner L2 —
// the writing node already did). It orphans the entity's query entries (local
// gen bump) and deletes the per-id row entries.
func (l *L1Cache) EvictLocal(ctx context.Context, entity string, ids ...string) {
	for _, part := range entityPartitions(ctx) {
		l.bumpGen(entity, part)
	}
	ent, ok := l.reg.Get(entity)
	if !ok || ent.Spec.Cache == nil {
		return
	}
	rowKs := cachequery.EntityRowKeyspace(ent)
	part, err := rowKs.Partition.Resolve(ctx)
	// Best-effort: a malformed tenant/scope ctx (which a committed envelope's
	// validated TenantID should never produce) leaves row entries to the L1 TTL.
	if err != nil {
		return
	}
	for _, id := range ids {
		l.store.del(l.l1key(rowKs, part, id))
	}
}

// Close delegates to the inner cache.
func (l *L1Cache) Close() error { return l.inner.Close() }
