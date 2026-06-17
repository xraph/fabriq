package query

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// listRow is the grove-tagged model used in list-cache tests.
type listRow struct {
	grove.BaseModel `grove:"table:list_rows"`

	ID       string `json:"id" grove:"id,pk"`
	TenantID string `json:"tenant_id" grove:"tenant_id,notnull"`
	Version  int64  `json:"version" grove:"version,notnull"`
	Name     string `json:"name" grove:"name"`
}

// relStub records List/GetMany calls and serves canned rows.
type relStub struct {
	mu       sync.Mutex
	lists    int
	getManys int
	rows     map[string]listRow
	order    []string // ids List returns, in order
}

func (s *relStub) Get(context.Context, string, string, any) error { return nil }
func (s *relStub) GetMany(_ context.Context, _ string, ids []string, into any) error {
	s.mu.Lock()
	s.getManys++
	s.mu.Unlock()
	out := into.(*[]*listRow)
	for _, id := range ids {
		if r, ok := s.rows[id]; ok {
			rc := r
			*out = append(*out, &rc)
		}
	}
	return nil
}
func (s *relStub) List(_ context.Context, _ string, _ ListQuery, into any) error {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	out := into.(*[]*listRow)
	for _, id := range s.order {
		r := s.rows[id]
		rc := r
		*out = append(*out, &rc)
	}
	return nil
}
func (s *relStub) Query(context.Context, any, string, ...any) error { return nil }

// listTestCache is a minimal in-memory cache.Cache for list-cache unit tests.
// It avoids importing fabriqtest (which imports core/query → cycle).
type listTestCache struct {
	mu   sync.Mutex
	data map[string][]byte
	gen  map[string]int64
}

func newListTestCache() *listTestCache {
	return &listTestCache{data: map[string][]byte{}, gen: map[string]int64{}}
}

func (c *listTestCache) genKey(ks cache.Keyspace, part string) string {
	if ks.Entity != "" {
		return "e|" + ks.Entity + "|" + part
	}
	return "k|" + ks.Name + "|" + part
}

func (c *listTestCache) fullKey(ks cache.Keyspace, part string, gen int64, key string) string {
	return ks.Name + "|v" + strconv.Itoa(ks.Version) + "|g" + strconv.FormatInt(gen, 10) + "|" + part + "|" + key
}

func (c *listTestCache) resolve(ctx context.Context, ks cache.Keyspace) (string, int64, error) {
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return "", 0, err
	}
	c.mu.Lock()
	gen := c.gen[c.genKey(ks, part)]
	c.mu.Unlock()
	return part, gen, nil
}

func (c *listTestCache) Get(ctx context.Context, ks cache.Keyspace, key string) ([]byte, bool, error) {
	part, gen, err := c.resolve(ctx, ks)
	if err != nil {
		return nil, false, err
	}
	fk := c.fullKey(ks, part, gen, key)
	c.mu.Lock()
	v, ok := c.data[fk]
	c.mu.Unlock()
	return v, ok, nil
}

func (c *listTestCache) Set(ctx context.Context, ks cache.Keyspace, key string, val []byte) error {
	part, gen, err := c.resolve(ctx, ks)
	if err != nil {
		return err
	}
	fk := c.fullKey(ks, part, gen, key)
	c.mu.Lock()
	c.data[fk] = val
	c.mu.Unlock()
	return nil
}

func (c *listTestCache) GetOrLoad(ctx context.Context, ks cache.Keyspace, key string, load func(context.Context) ([]byte, error)) ([]byte, error) {
	if v, ok, err := c.Get(ctx, ks, key); err != nil || ok {
		return v, err
	}
	val, err := load(ctx)
	if err != nil {
		return nil, err
	}
	_ = c.Set(ctx, ks, key, val)
	return val, nil
}

func (c *listTestCache) Invalidate(ctx context.Context, ks cache.Keyspace, keys ...string) error {
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

func (c *listTestCache) InvalidateKeyspace(ctx context.Context, ks cache.Keyspace) error {
	part, err := ks.Partition.Resolve(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.gen[c.genKey(ks, part)]++
	c.mu.Unlock()
	return nil
}

// InvalidateEntity bumps the entity generation across partitions (mirrors fabriqtest.FakeCache).
func (c *listTestCache) InvalidateEntity(ctx context.Context, entity string) error {
	tid, err := tenant.FromContext(ctx)
	if err != nil {
		// No tenant: only global.
		c.mu.Lock()
		c.gen["e|"+entity+"|g"]++
		c.mu.Unlock()
		return nil
	}
	c.mu.Lock()
	c.gen["e|"+entity+"|g"]++
	c.gen["e|"+entity+"|t:"+tid]++
	c.gen["e|"+entity+"|t:"+tid+":s:"+tenant.ScopeOrEmpty(ctx)]++
	c.mu.Unlock()
	return nil
}

func (c *listTestCache) Close() error { return nil }

var _ cache.Cache = (*listTestCache)(nil)

// Ensure listTestCache stores valid JSON (used indirectly through GetOrLoad).
var _ = json.Marshal

func newCachedListRepo(t *testing.T, rel RelationalQuerier, c cache.Cache) *Repo[listRow] {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{Name: "asset", Kind: registry.KindAggregate,
		Model: listRow{}, Cache: &registry.CacheSpec{TTL: time.Minute}}); err != nil {
		t.Fatal(err)
	}
	repo, err := For[listRow](reg, rel)
	if err != nil {
		t.Fatal(err)
	}
	ks := cache.Keyspace{Name: "asset:q", Version: 1, Entity: "asset",
		Partition: cache.Tenant, Policy: cache.Policy{Mode: cache.Versioned, TTL: time.Minute}}
	return repo.WithResultCache(c, ks)
}

func TestList_CachedResultSet(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{
		"a1": {ID: "a1", Name: "Pump"}, "a2": {ID: "a2", Name: "Valve"},
	}, order: []string{"a1", "a2"}}
	fc := newListTestCache()
	repo := newCachedListRepo(t, rel, fc)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	first, err := repo.List(ctx, ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || rel.lists != 1 {
		t.Fatalf("first list: rows=%d lists=%d", len(first), rel.lists)
	}
	// Second identical List hits the id-list cache: underlying List NOT called again.
	second, err := repo.List(ctx, ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || rel.lists != 1 {
		t.Fatalf("second list should hit cache: rows=%d lists=%d (want lists=1)", len(second), rel.lists)
	}
}

func TestList_InvalidateEntityBustsResultSet(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"a1": {ID: "a1"}}, order: []string{"a1"}}
	fc := newListTestCache()
	repo := newCachedListRepo(t, rel, fc)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	_, _ = repo.List(ctx, ListQuery{})
	// A write to the entity bumps the generation → id-list cache busts.
	if err := fc.InvalidateEntity(ctx, "asset"); err != nil {
		t.Fatal(err)
	}
	_, _ = repo.List(ctx, ListQuery{})
	if rel.lists != 2 {
		t.Fatalf("after InvalidateEntity, List must re-run: lists=%d (want 2)", rel.lists)
	}
}
