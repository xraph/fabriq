package shard_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// stub implements all four source-of-truth ports and records the calls it
// received, so a test can assert which shard a tenant routed to.
type stub struct {
	id    string
	calls []string
}

func (s *stub) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	s.calls = append(s.calls, "InTenantTx")
	return fn(ctx, nil)
}
func (s *stub) Get(_ context.Context, entity, _ string, _ any) error {
	s.calls = append(s.calls, "Get:"+entity)
	return nil
}
func (s *stub) GetMany(_ context.Context, entity string, _ []string, _ any) error {
	s.calls = append(s.calls, "GetMany:"+entity)
	return nil
}
func (s *stub) List(_ context.Context, entity string, _ query.ListQuery, _ any) error {
	s.calls = append(s.calls, "List:"+entity)
	return nil
}
func (s *stub) Query(_ context.Context, _ any, _ string, _ ...any) error {
	s.calls = append(s.calls, "Query")
	return nil
}
func (s *stub) Upsert(_ context.Context, entity, _ string, _ []float32, _ map[string]any) error {
	s.calls = append(s.calls, "Upsert:"+entity)
	return nil
}
func (s *stub) Similar(_ context.Context, q query.VectorQuery, _ any) error {
	s.calls = append(s.calls, "Similar:"+q.Entity)
	return nil
}
func (s *stub) BulkWrite(_ context.Context, series string, _ []query.Point) error {
	s.calls = append(s.calls, "BulkWrite:"+series)
	return nil
}
func (s *stub) Range(_ context.Context, q query.RangeQuery, _ any) error {
	s.calls = append(s.calls, "Range:"+q.Series)
	return nil
}

func shardFor(s *stub) shard.Shard {
	return shard.Shard{ID: s.id, Store: s, Relational: s, Vector: s, Timeseries: s}
}

// mapDir routes tenant ids to shard ids; an unmapped tenant errors.
type mapDir map[string]string

func (m mapDir) Shard(_ context.Context, tenantID string) (string, error) {
	id, ok := m[tenantID]
	if !ok {
		return "", fmt.Errorf("no shard for tenant %q", tenantID)
	}
	return id, nil
}

func ctxFor(t *testing.T, tid string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tid)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestSet_RoutesByTenant(t *testing.T) {
	s0, s1 := &stub{id: "0"}, &stub{id: "1"}
	set, err := shard.New(mapDir{"acme": "0", "globex": "1"}, shardFor(s0), shardFor(s1))
	if err != nil {
		t.Fatal(err)
	}

	rel := shard.NewRelational(set)
	// acme -> shard 0
	if err := rel.Get(ctxFor(t, "acme"), "asset", "A1", nil); err != nil {
		t.Fatal(err)
	}
	// globex -> shard 1
	if err := rel.List(ctxFor(t, "globex"), "asset", query.ListQuery{}, nil); err != nil {
		t.Fatal(err)
	}

	if len(s0.calls) != 1 || s0.calls[0] != "Get:asset" {
		t.Fatalf("shard 0 calls = %v", s0.calls)
	}
	if len(s1.calls) != 1 || s1.calls[0] != "List:asset" {
		t.Fatalf("shard 1 calls = %v", s1.calls)
	}
}

func TestSet_AllPortsDelegate(t *testing.T) {
	s := &stub{id: "0"}
	set := shard.Single(shardFor(s))
	ctx := ctxFor(t, "acme")

	if err := shard.NewStore(set).InTenantTx(ctx, func(context.Context, command.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	rel := shard.NewRelational(set)
	_ = rel.GetMany(ctx, "asset", []string{"A1"}, nil)
	_ = rel.Query(ctx, nil, "SELECT 1")
	_ = shard.NewVector(set).Upsert(ctx, "asset", "A1", []float32{1}, nil)
	_ = shard.NewVector(set).Similar(ctx, query.VectorQuery{Entity: "asset"}, nil)
	_ = shard.NewTimeseries(set).BulkWrite(ctx, "tag_readings", nil)
	_ = shard.NewTimeseries(set).Range(ctx, query.RangeQuery{Series: "tag_readings"}, nil)

	want := []string{"InTenantTx", "GetMany:asset", "Query", "Upsert:asset", "Similar:asset", "BulkWrite:tag_readings", "Range:tag_readings"}
	if fmt.Sprint(s.calls) != fmt.Sprint(want) {
		t.Fatalf("calls = %v, want %v", s.calls, want)
	}
}

func TestSet_Single_RoutesEveryTenant(t *testing.T) {
	s := &stub{id: "0"}
	set := shard.Single(shardFor(s))
	if set.Len() != 1 {
		t.Fatalf("Len = %d", set.Len())
	}
	rel := shard.NewRelational(set)
	for _, tid := range []string{"acme", "globex", "initech"} {
		if err := rel.Get(ctxFor(t, tid), "asset", "x", nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.calls) != 3 {
		t.Fatalf("single shard should have taken all 3 reads: %v", s.calls)
	}
}

func TestSet_For_Errors(t *testing.T) {
	s := &stub{id: "0"}
	rel := shard.NewRelational(shard.Single(shardFor(s)))

	// No tenant on ctx -> error (the source-of-truth precondition).
	if err := rel.Get(context.Background(), "asset", "x", nil); err == nil {
		t.Fatal("missing tenant must error")
	}

	// Directory names a shard the set does not hold.
	set, err := shard.New(mapDir{"acme": "missing"}, shardFor(s))
	if err != nil {
		t.Fatal(err)
	}
	if err := shard.NewRelational(set).Get(ctxFor(t, "acme"), "asset", "x", nil); err == nil {
		t.Fatal("routing to an unknown shard must error")
	}

	// Directory error propagates (unmapped tenant).
	set2, _ := shard.New(mapDir{"acme": "0"}, shardFor(s))
	if err := shard.NewRelational(set2).Get(ctxFor(t, "stranger"), "asset", "x", nil); err == nil {
		t.Fatal("unmapped tenant must error")
	}
}

func TestNew_Validation(t *testing.T) {
	s := &stub{id: "0"}
	if _, err := shard.New(nil, shardFor(s)); err == nil {
		t.Fatal("nil directory must error")
	}
	if _, err := shard.New(mapDir{}); err == nil {
		t.Fatal("zero shards must error")
	}
	if _, err := shard.New(mapDir{}, shard.Shard{ID: ""}); err == nil {
		t.Fatal("empty shard id must error")
	}
	if _, err := shard.New(mapDir{}, shardFor(s), shardFor(s)); err == nil {
		t.Fatal("duplicate shard id must error")
	}
}

func TestSet_All_SortedByID(t *testing.T) {
	a, b, c := &stub{id: "a"}, &stub{id: "b"}, &stub{id: "c"}
	set, err := shard.New(mapDir{}, shardFor(c), shardFor(a), shardFor(b))
	if err != nil {
		t.Fatal(err)
	}
	got := set.All()
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Fatalf("All not sorted: %+v", got)
	}
}
