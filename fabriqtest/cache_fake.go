package fabriqtest

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/tenant"
)

// FakeCache is an in-memory cache.Cache for unit tests. It implements the same
// behavioral contract (RunCacheConformance) as the real grove-kv adapter:
// scope isolation, generation invalidation, single-flight GetOrLoad, TTL.
type FakeCache struct {
	mu     sync.Mutex
	data   map[string]fakeEntry
	gen    map[string]int64
	flight map[string]*fakeCall
	now    func() time.Time // injectable clock (defaults to time.Now)
}

type fakeEntry struct {
	val []byte
	exp time.Time // zero = no expiry
}

type fakeCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

// NewFakeCache builds an empty in-memory cache.
func NewFakeCache() *FakeCache {
	return &FakeCache{
		data:   map[string]fakeEntry{},
		gen:    map[string]int64{},
		flight: map[string]*fakeCall{},
		now:    time.Now,
	}
}

// genKey is the generation-counter key for ks under partition `part`.
// Entity-keyed when ks.Entity is set (so InvalidateEntity can bump it), else
// keyspace-name-keyed (the P1 behavior, for keyspaces not tied to one entity).
func (c *FakeCache) genKey(ks cache.Keyspace, part string) string {
	if ks.Entity != "" {
		return "e|" + ks.Entity + "|" + part
	}
	return "k|" + ks.Name + "|" + part
}

func (c *FakeCache) fullKey(ks cache.Keyspace, part string, gen int64, key string) (result string) {
	return ks.Name + "|v" + strconv.Itoa(ks.Version) + "|g" + strconv.FormatInt(gen, 10) +
		"|" + part + "|" + key
}

// resolve returns (partition segment, current generation) for ks under ctx.
func (c *FakeCache) resolve(ctx context.Context, ks cache.Keyspace) (part string, gen int64, err error) {
	part, err = ks.Partition.Resolve(ctx)
	if err != nil {
		return
	}
	c.mu.Lock()
	gen = c.gen[c.genKey(ks, part)]
	c.mu.Unlock()
	return
}

func (c *FakeCache) Get(ctx context.Context, ks cache.Keyspace, key string) (val []byte, ok bool, err error) {
	var part string
	var gen int64
	part, gen, err = c.resolve(ctx, ks)
	if err != nil {
		return
	}
	fk := c.fullKey(ks, part, gen, key)
	c.mu.Lock()
	defer c.mu.Unlock()
	var e fakeEntry
	e, ok = c.data[fk]
	if !ok {
		return
	}
	if !e.exp.IsZero() && c.now().After(e.exp) {
		delete(c.data, fk)
		ok = false
		return
	}
	val = e.val
	return
}

func (c *FakeCache) Set(ctx context.Context, ks cache.Keyspace, key string, val []byte) error {
	part, gen, err := c.resolve(ctx, ks)
	if err != nil {
		return err
	}
	fk := c.fullKey(ks, part, gen, key)
	var exp time.Time
	if ks.Policy.TTL > 0 {
		exp = c.now().Add(ks.Policy.TTL)
	}
	c.mu.Lock()
	c.data[fk] = fakeEntry{val: val, exp: exp}
	c.mu.Unlock()
	return nil
}

func (c *FakeCache) GetOrLoad(ctx context.Context, ks cache.Keyspace, key string,
	load func(context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok, err := c.Get(ctx, ks, key); err != nil || ok {
		return v, err
	}
	part, gen, err := c.resolve(ctx, ks)
	if err != nil {
		return nil, err
	}
	fk := c.fullKey(ks, part, gen, key)

	// Single-flight: first caller loads, others wait and share the result.
	c.mu.Lock()
	if call, ok := c.flight[fk]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	call := &fakeCall{}
	call.wg.Add(1)
	c.flight[fk] = call
	c.mu.Unlock()

	call.val, call.err = load(ctx)
	if call.err == nil {
		_ = c.Set(ctx, ks, key, call.val)
	}
	c.mu.Lock()
	delete(c.flight, fk)
	c.mu.Unlock()
	call.wg.Done()
	return call.val, call.err
}

func (c *FakeCache) Invalidate(ctx context.Context, ks cache.Keyspace, keys ...string) error {
	part, gen, err := c.resolve(ctx, ks)
	if err != nil {
		return err
	}
	c.mu.Lock()
	for _, k := range keys {
		delete(c.data, c.fullKey(ks, part, gen, k))
	}
	c.mu.Unlock()
	return nil
}

func (c *FakeCache) InvalidateKeyspace(ctx context.Context, ks cache.Keyspace) error {
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.gen[c.genKey(ks, part)]++
	c.mu.Unlock()
	return nil
}

// InvalidateEntity bumps the entity generation for Global + Tenant + (if
// present) TenantScope partitions, mirroring the adapter.
func (c *FakeCache) InvalidateEntity(ctx context.Context, entity string) error {
	parts, err := entityPartitions(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	for _, part := range parts {
		c.gen["e|"+entity+"|"+part]++
	}
	c.mu.Unlock()
	return nil
}

func (c *FakeCache) Close() error { return nil }

// entityPartitions returns the partition segments a write under ctx must bump:
// always Global; Tenant when a tenant is present; TenantScope when a scope is
// also present. Segments match cache.Partition.Resolve output.
//
//nolint:unparam // error result is by design: reserved for adapter consistency (Task 2)
func entityPartitions(ctx context.Context) ([]string, error) {
	parts := []string{"g"}
	tid, err := tenant.FromContext(ctx)
	if err != nil {
		// No tenant: only the global tier can be affected.
		return parts, nil
	}
	parts = append(parts, "t:"+tid)
	if scope := tenant.ScopeOrEmpty(ctx); scope != "" {
		parts = append(parts, "t:"+tid+":s:"+scope)
	}
	return parts, nil
}

var _ cache.Cache = (*FakeCache)(nil)
