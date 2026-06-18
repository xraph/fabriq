package cache

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/grove"

	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// memCache is an in-memory cache.Cache standing in for the L2, counting calls.
type memCache struct {
	data map[string][]byte
	gets int
	gen  map[string]int
}

func newMem() *memCache { return &memCache{data: map[string][]byte{}, gen: map[string]int{}} }

func (m *memCache) gkey(ks corecache.Keyspace, part, key string) string {
	src := ks.Name
	if ks.Entity != "" {
		src = ks.Entity
	}
	return ks.Name + "|" + part + "|g" + itoa(m.gen[src+"|"+part]) + "|" + key
}
func (m *memCache) Get(ctx context.Context, ks corecache.Keyspace, key string) ([]byte, bool, error) {
	m.gets++
	part, _ := ks.Partition.Resolve(ctx)
	v, ok := m.data[m.gkey(ks, part, key)]
	return v, ok, nil
}
func (m *memCache) Set(ctx context.Context, ks corecache.Keyspace, key string, val []byte) error {
	part, _ := ks.Partition.Resolve(ctx)
	m.data[m.gkey(ks, part, key)] = val
	return nil
}
func (m *memCache) GetOrLoad(ctx context.Context, ks corecache.Keyspace, key string, load func(context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok, _ := m.Get(ctx, ks, key); ok {
		return v, nil
	}
	v, err := load(ctx)
	if err != nil {
		return nil, err
	}
	_ = m.Set(ctx, ks, key, v)
	return v, nil
}
func (m *memCache) Invalidate(ctx context.Context, ks corecache.Keyspace, keys ...string) error {
	part, _ := ks.Partition.Resolve(ctx)
	for _, k := range keys {
		delete(m.data, m.gkey(ks, part, k))
	}
	return nil
}
func (m *memCache) InvalidateKeyspace(ctx context.Context, ks corecache.Keyspace) error {
	part, _ := ks.Partition.Resolve(ctx)
	src := ks.Name
	if ks.Entity != "" {
		src = ks.Entity
	}
	m.gen[src+"|"+part]++
	return nil
}
func (m *memCache) InvalidateEntity(ctx context.Context, entity string) error {
	for _, part := range entityPartitions(ctx) {
		m.gen[entity+"|"+part]++
	}
	return nil
}
func (m *memCache) Close() error { return nil }

func itoa(n int) string { // tiny local helper
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// assetModel is the minimal grove-tagged model satisfying registry structural
// requirements (id, tenant_id, version are all required for KindAggregate).
type assetModel struct {
	grove.BaseModel `grove:"table:assets"`
	ID              string `grove:"id,pk"          json:"id"`
	TenantID        string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version         int64  `grove:"version,notnull"   json:"version"`
}

func l1reg(t *testing.T) *registry.Registry {
	t.Helper()
	r := registry.New()
	if err := r.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: assetModel{},
		Cache: &registry.CacheSpec{TTL: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func l1ctx(t *testing.T) context.Context {
	t.Helper()
	c, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func rowKS() corecache.Keyspace {
	return corecache.Keyspace{Name: "asset:row", Version: 1, Partition: corecache.Tenant,
		Policy: corecache.Policy{Mode: corecache.EventEvict}}
}
func queryKS() corecache.Keyspace {
	return corecache.Keyspace{Name: "asset:q", Version: 1, Entity: "asset", Partition: corecache.Tenant,
		Policy: corecache.Policy{Mode: corecache.Versioned, TTL: time.Minute}}
}

func TestL1_ServesWarmAndPopulates(t *testing.T) {
	m := newMem()
	l := NewL1(m, l1reg(t), 64, time.Minute)
	ctx := l1ctx(t)
	_ = l.Set(ctx, rowKS(), "a1", []byte("v"))
	if _, _, err := l.Get(ctx, rowKS(), "a1"); err != nil {
		t.Fatal(err)
	}
	before := m.gets
	// Warm: served from L1, inner.Get NOT called again.
	if v, ok, _ := l.Get(ctx, rowKS(), "a1"); !ok || string(v) != "v" {
		t.Fatalf("warm get: v=%q ok=%v", v, ok)
	}
	if m.gets != before {
		t.Fatalf("L1 hit must not call inner.Get: gets went %d->%d", before, m.gets)
	}
}

func TestL1_InvalidateEntityOrphansQueryL1AndBumpsInner(t *testing.T) {
	m := newMem()
	l := NewL1(m, l1reg(t), 64, time.Minute)
	ctx := l1ctx(t)
	_ = l.Set(ctx, queryKS(), "fp1", []byte("ids"))
	if _, ok, _ := l.Get(ctx, queryKS(), "fp1"); !ok {
		t.Fatal("precondition: query entry cached in L1")
	}
	if err := l.InvalidateEntity(ctx, "asset"); err != nil {
		t.Fatal(err)
	}
	// L1 query entry orphaned (local gen bumped); inner generation also bumped.
	if _, ok, _ := l.Get(ctx, queryKS(), "fp1"); ok {
		t.Fatal("query L1 entry must be orphaned after InvalidateEntity")
	}
	if m.gen["asset|t:acme"] == 0 {
		t.Fatal("inner InvalidateEntity must have been delegated (gen bumped)")
	}
}

func TestL1_PerIDRowEviction_SiblingsWarm(t *testing.T) {
	m := newMem()
	l := NewL1(m, l1reg(t), 64, time.Minute)
	ctx := l1ctx(t)
	_ = l.Set(ctx, rowKS(), "a1", []byte("1"))
	_ = l.Set(ctx, rowKS(), "a2", []byte("2"))
	if err := l.Invalidate(ctx, rowKS(), "a1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := l.Get(ctx, rowKS(), "a1"); ok {
		t.Fatal("evicted row a1 must miss L1")
	}
	// Sibling a2 must remain warm in L1 (served without inner.Get).
	before := m.gets
	if _, ok, _ := l.Get(ctx, rowKS(), "a2"); !ok {
		t.Fatal("sibling a2 must stay warm")
	}
	if m.gets != before {
		t.Fatal("sibling a2 should be served from L1 (no inner.Get)")
	}
}

func TestL1_EvictLocal_OrphansQueryAndRow_NoInner(t *testing.T) {
	m := newMem()
	l := NewL1(m, l1reg(t), 64, time.Minute)
	ctx := l1ctx(t)
	_ = l.Set(ctx, queryKS(), "fp1", []byte("ids"))
	_ = l.Set(ctx, rowKS(), "a1", []byte("v"))
	genBefore := m.gen["asset|t:acme"]
	l.EvictLocal(ctx, "asset", "a1")
	if _, ok, _ := l.Get(ctx, queryKS(), "fp1"); ok {
		t.Fatal("EvictLocal must orphan query L1 for the entity")
	}
	if _, ok, _ := l.Get(ctx, rowKS(), "a1"); ok {
		t.Fatal("EvictLocal must evict the row L1 entry")
	}
	if m.gen["asset|t:acme"] != genBefore {
		t.Fatal("EvictLocal must NOT touch the inner/L2 generation (local-only)")
	}
}
