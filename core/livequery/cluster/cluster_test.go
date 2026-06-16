package cluster

import (
	"fmt"
	"testing"
)

func TestPartitionOf_StableAndBounded(t *testing.T) {
	p := PartitionOf("acme", "asset")
	if p < 0 || p >= Partitions {
		t.Fatalf("partition %d out of range [0,%d)", p, Partitions)
	}
	if PartitionOf("acme", "asset") != p {
		t.Fatal("PartitionOf must be deterministic")
	}
	if EventStream(p) != fmt.Sprintf("lq:events:%d", p) {
		t.Fatalf("EventStream = %q", EventStream(p))
	}
	// Different (tenant, entity) generally land on different partitions.
	if PartitionOf("acme", "asset") == PartitionOf("acme", "site") &&
		PartitionOf("acme", "asset") == PartitionOf("rival", "asset") {
		t.Skip("hash collision across the three probes (rare); not a failure")
	}
}

func TestPartitionOf_RoughlyUniform(t *testing.T) {
	counts := make([]int, Partitions)
	const n = 20000
	for i := 0; i < n; i++ {
		counts[PartitionOf(fmt.Sprintf("t%d", i%200), fmt.Sprintf("e%d", i))]++
	}
	empty := 0
	for _, c := range counts {
		if c == 0 {
			empty++
		}
	}
	// With 20k keys over 256 partitions (~78 each), essentially none should be empty.
	if empty > Partitions/10 {
		t.Fatalf("%d/%d partitions empty — distribution too skewed", empty, Partitions)
	}
}

func TestHRW_OwnerDeterministicAndCovered(t *testing.T) {
	live := []string{"shard-a", "shard-b", "shard-c"}
	owners := map[string]bool{}
	for p := 0; p < Partitions; p++ {
		o := Owner(p, live)
		if o == "" {
			t.Fatalf("partition %d has no owner", p)
		}
		owners[o] = true
		if Owner(p, live) != o {
			t.Fatal("Owner must be deterministic")
		}
		if !Owns(o, p, live) {
			t.Fatalf("Owns(%s,%d) false but Owner said %s", o, p, o)
		}
	}
	if len(owners) != 3 {
		t.Fatalf("only %d/3 shards own any partition (unbalanced)", len(owners))
	}
}

func TestHRW_MinimalChurnOnRemoval(t *testing.T) {
	full := []string{"shard-a", "shard-b", "shard-c", "shard-d"}
	reduced := []string{"shard-a", "shard-b", "shard-c"} // shard-d removed

	moved, ownedByD := 0, 0
	for p := 0; p < Partitions; p++ {
		before := Owner(p, full)
		after := Owner(p, reduced)
		if before == "shard-d" {
			ownedByD++
		}
		if before != after {
			moved++
			// Only partitions previously owned by the removed shard may move.
			if before != "shard-d" {
				t.Fatalf("partition %d moved from %s to %s — HRW must not disturb others", p, before, after)
			}
		}
	}
	if moved != ownedByD {
		t.Fatalf("moved %d but %d were owned by removed shard — churn not minimal", moved, ownedByD)
	}
	if ownedByD == 0 {
		t.Fatal("removed shard owned no partitions (test setup wrong)")
	}
}

func TestHRW_BalanceAcrossShards(t *testing.T) {
	live := []string{"s0", "s1", "s2", "s3", "s4"}
	counts := map[string]int{}
	for p := 0; p < Partitions; p++ {
		counts[Owner(p, live)]++
	}
	ideal := Partitions / len(live)
	for s, c := range counts {
		if c < ideal/2 || c > ideal*2 {
			t.Fatalf("shard %s owns %d partitions, ideal ~%d — too unbalanced", s, c, ideal)
		}
	}
}
