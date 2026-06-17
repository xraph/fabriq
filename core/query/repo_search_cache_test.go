package query

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// searchStub is a fake SearchQuerier for search-cache unit tests.
// Search sets *into to the canned hits and counts calls; ApplyMutations is a no-op.
type searchStub struct {
	mu       sync.Mutex
	searches int
	hits     []map[string]any
}

func (s *searchStub) Search(_ context.Context, _ SearchQuery, into any) error {
	s.mu.Lock()
	s.searches++
	s.mu.Unlock()
	*(into.(*[]map[string]any)) = s.hits
	return nil
}

func (s *searchStub) ApplyMutations(_ context.Context, _ string, _ []projection.Mutation) error {
	return nil
}

// vectorStub is a fake VectorQuerier for similar-cache unit tests.
// Similar sets *into to the canned matches and counts calls; Upsert is a no-op.
type vectorStub struct {
	mu      sync.Mutex
	calls   int
	matches []VectorMatch
}

func (v *vectorStub) Upsert(_ context.Context, _, _ string, _ []float32, _ map[string]any) error {
	return nil
}

func (v *vectorStub) Similar(_ context.Context, _ VectorQuery, into any) error {
	v.mu.Lock()
	v.calls++
	v.mu.Unlock()
	*(into.(*[]VectorMatch)) = v.matches
	return nil
}

// TestSearchWith_CachedIDList: two identical SearchWith calls → underlying Search
// called ONCE (ss.searches==1 after the second call).
func TestSearchWith_CachedIDList(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"a1": {ID: "a1"}}}
	ss := &searchStub{hits: []map[string]any{{"id": "a1"}}}
	fc := newListTestCache()
	repo := newCachedListRepo(t, rel, fc).WithSearch(ss)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	if _, err := repo.SearchWith(ctx, SearchRequest{Query: "pump"}); err != nil {
		t.Fatal(err)
	}
	if ss.searches != 1 {
		t.Fatalf("first SearchWith: searches=%d (want 1)", ss.searches)
	}
	// Second identical SearchWith hits the id-list cache: underlying Search NOT called again.
	if _, err := repo.SearchWith(ctx, SearchRequest{Query: "pump"}); err != nil {
		t.Fatal(err)
	}
	if ss.searches != 1 {
		t.Fatalf("second identical SearchWith should hit cache: searches=%d (want 1)", ss.searches)
	}
}

// newNoCacheRepo builds a Repo[listRow] with NO result cache, using the same
// registry setup as newCachedListRepo so nil-cache path tests can reuse relStub.
func newNoCacheRepo(t *testing.T, rel RelationalQuerier) *Repo[listRow] {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: listRow{},
	}); err != nil {
		t.Fatal(err)
	}
	repo, err := For[listRow](reg, rel)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// TestSearchWith_NilCache: with no cache wired the non-cached path is used —
// Search is called on every SearchWith call.
func TestSearchWith_NilCache(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"a1": {ID: "a1"}}}
	ss := &searchStub{hits: []map[string]any{{"id": "a1"}}}
	repo := newNoCacheRepo(t, rel).WithSearch(ss)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	if _, err := repo.SearchWith(ctx, SearchRequest{Query: "pump"}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SearchWith(ctx, SearchRequest{Query: "pump"}); err != nil {
		t.Fatal(err)
	}
	if ss.searches != 2 {
		t.Fatalf("nil-cache SearchWith should call Search each time: searches=%d (want 2)", ss.searches)
	}
}

// TestSimilar_CachedIDList: two identical Similar calls → underlying Similar
// called ONCE (vs.calls==1 after the second call).
func TestSimilar_CachedIDList(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"v1": {ID: "v1"}}}
	vs := &vectorStub{matches: []VectorMatch{{ID: "v1", Score: 0.99}}}
	fc := newListTestCache()
	repo := newCachedListRepo(t, rel, fc).WithVector(vs)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	emb := []float32{0.1, 0.2, 0.3}
	if _, err := repo.Similar(ctx, emb, 5); err != nil {
		t.Fatal(err)
	}
	if vs.calls != 1 {
		t.Fatalf("first Similar: calls=%d (want 1)", vs.calls)
	}
	// Second identical Similar hits the id-list cache: underlying Similar NOT called again.
	if _, err := repo.Similar(ctx, emb, 5); err != nil {
		t.Fatal(err)
	}
	if vs.calls != 1 {
		t.Fatalf("second identical Similar should hit cache: calls=%d (want 1)", vs.calls)
	}
}

// TestSimilar_NilCache: with no cache wired the non-cached path is used —
// vector.Similar is called on every Similar call.
func TestSimilar_NilCache(t *testing.T) {
	rel := &relStub{rows: map[string]listRow{"v1": {ID: "v1"}}}
	vs := &vectorStub{matches: []VectorMatch{{ID: "v1", Score: 0.99}}}
	repo := newNoCacheRepo(t, rel).WithVector(vs)
	ctx, _ := tenant.WithTenant(context.Background(), "acme")

	emb := []float32{0.1, 0.2, 0.3}
	if _, err := repo.Similar(ctx, emb, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Similar(ctx, emb, 5); err != nil {
		t.Fatal(err)
	}
	if vs.calls != 2 {
		t.Fatalf("nil-cache Similar should call vector.Similar each time: calls=%d (want 2)", vs.calls)
	}
}
