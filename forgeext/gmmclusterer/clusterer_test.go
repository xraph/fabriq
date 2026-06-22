package gmmclusterer

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/agent"
)

// TestKMeansClusterer_GroupsAndStableIDs: two well-separated vector groups (≥ floor
// each) yield 2 clusters with stable, deterministic, cluster-namespaced ids.
func TestKMeansClusterer_GroupsAndStableIDs(t *testing.T) {
	c := New(2, 4) // k=2, idBits=4
	inputs := []agent.ClusterInput{
		{ID: "a", Vector: []float32{0, 0}}, {ID: "b", Vector: []float32{0, 0.1}},
		{ID: "x", Vector: []float32{10, 10}}, {ID: "y", Vector: []float32{10, 10.1}},
	}
	got1, err := c.Cluster(context.Background(), inputs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got1) != 2 {
		t.Fatalf("want 2 clusters, got %d", len(got1))
	}
	for _, cl := range got1 {
		if !agentIsCluster(cl.ID) {
			t.Fatalf("cluster id %q must be a cluster-node id", cl.ID)
		}
	}
	got2, _ := c.Cluster(context.Background(), inputs, 2)
	if !sameClusters(got1, got2) {
		t.Fatal("clustering must be deterministic (stable ids + membership)")
	}
	if !c.NeedsVectors() {
		t.Fatal("vector clusterer must need vectors")
	}
}

// agentIsCluster checks that id is a cluster-node id (digest:1:cluster:*).
func agentIsCluster(id string) bool {
	return strings.HasPrefix(id, "digest:1:cluster:")
}

// sameClusters compares two cluster slices for identical ids and membership
// (order-insensitive within each cluster, order-insensitive across clusters).
func sameClusters(a, b []agent.Cluster) bool {
	if len(a) != len(b) {
		return false
	}
	byID := func(cs []agent.Cluster) map[string][]string {
		m := make(map[string][]string, len(cs))
		for _, c := range cs {
			mems := make([]string, len(c.Members))
			copy(mems, c.Members)
			sort.Strings(mems)
			m[c.ID] = mems
		}
		return m
	}
	ma, mb := byID(a), byID(b)
	for id, mems := range ma {
		bMems, ok := mb[id]
		if !ok {
			return false
		}
		if len(mems) != len(bMems) {
			return false
		}
		for i := range mems {
			if mems[i] != bMems[i] {
				return false
			}
		}
	}
	return true
}
