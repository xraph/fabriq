package shard_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/shard"
)

func TestHashDirectory_DeterministicAndCovers(t *testing.T) {
	dir := shard.HashDirectory("b", "a", "c") // unsorted on purpose
	ctx := context.Background()

	// Deterministic: same tenant -> same shard across calls.
	first := make(map[string]string)
	seen := map[string]bool{}
	for i := 0; i < 300; i++ {
		tid := fmt.Sprintf("tenant-%d", i)
		id, err := dir.Shard(ctx, tid)
		if err != nil {
			t.Fatal(err)
		}
		if id != "a" && id != "b" && id != "c" {
			t.Fatalf("tenant %s -> unknown shard %q", tid, id)
		}
		if prev, ok := first[tid]; ok && prev != id {
			t.Fatalf("non-deterministic: %s -> %s then %s", tid, prev, id)
		}
		first[tid] = id
		seen[id] = true
	}
	// Over 300 tenants all three shards should see traffic.
	if len(seen) != 3 {
		t.Fatalf("hash directory did not spread across all shards: %v", seen)
	}
}

func TestHashDirectory_Empty(t *testing.T) {
	if _, err := shard.HashDirectory().Shard(context.Background(), "acme"); err == nil {
		t.Fatal("empty hash directory must error")
	}
}

// countingDir records how many times the inner directory is consulted, to
// prove the cache short-circuits.
type countingDir struct {
	id    string
	calls int
}

func (c *countingDir) Shard(context.Context, string) (string, error) {
	c.calls++
	return c.id, nil
}

func TestCached_MemoizesWithinTTL(t *testing.T) {
	inner := &countingDir{id: "0"}
	dir := shard.Cached(inner, time.Hour)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := dir.Shard(ctx, "acme"); err != nil {
			t.Fatal(err)
		}
	}
	if inner.calls != 1 {
		t.Fatalf("cache should have consulted inner once, got %d", inner.calls)
	}
	// A different tenant is a separate cache entry.
	if _, err := dir.Shard(ctx, "globex"); err != nil {
		t.Fatal(err)
	}
	if inner.calls != 2 {
		t.Fatalf("new tenant should consult inner, got %d", inner.calls)
	}
}

func TestSet_ResolveID_And_ForTenant(t *testing.T) {
	s0, s1 := &stub{id: "0"}, &stub{id: "1"}
	set, err := shard.New(shard.HashDirectory("0", "1"), shardFor(s0), shardFor(s1))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// ResolveID and ForTenant agree, and route to a real shard.
	id, err := set.ResolveID(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	sh, err := set.ForTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if sh.ID != id {
		t.Fatalf("ForTenant id %q != ResolveID %q", sh.ID, id)
	}
	if got := set.IDs(); len(got) != 2 || got[0] != "0" || got[1] != "1" {
		t.Fatalf("IDs = %v", got)
	}
}
