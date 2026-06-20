package agent

import "testing"

func TestDigestIDDerivation(t *testing.T) {
	if L0ID("asset", "a1") != "digest:0:asset:a1" {
		t.Fatalf("L0 id: %s", L0ID("asset", "a1"))
	}
	if ScopeID("site", "s1") != "digest:1:scope:site:s1" {
		t.Fatalf("scope id: %s", ScopeID("site", "s1"))
	}
	if TenantRootID() != "digest:2:tenant" {
		t.Fatalf("tenant id: %s", TenantRootID())
	}
	// Cluster id is stable for the same prefix/p (membership drift must not move it).
	first := ClusterID(uint64(0xF)<<60, 4)
	again := ClusterID(uint64(0xF)<<60, 4)
	if first != again {
		t.Fatal("cluster id must be stable")
	}
	if ClusterID(uint64(0xF)<<60, 4) == ClusterID(uint64(0xE)<<60, 4) {
		t.Fatal("different prefixes must yield different cluster ids")
	}
}

func TestNoiseFloor(t *testing.T) {
	if NoiseFloorMet(1, 2) {
		t.Fatal("a singleton must not meet a floor of 2")
	}
	if !NoiseFloorMet(2, 2) {
		t.Fatal("2 members must meet a floor of 2")
	}
}
