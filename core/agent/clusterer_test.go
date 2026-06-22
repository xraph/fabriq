package agent

import (
	"context"
	"testing"
)

// TestSimHashClusterer_FloorAndProbe: only above-floor primary buckets become
// clusters; ProbeRadius widens an existing cluster but never creates one.
func TestSimHashClusterer_PrimaryFloor(t *testing.T) {
	c := NewSimHashClusterer(4, 0) // no probe
	// Two inputs sharing the top-4-bit prefix → one above-floor cluster (floor 2).
	p := ClusterPrefix(^uint64(0), 4)
	inputs := []ClusterInput{
		{ID: "a", SemHash: p}, {ID: "b", SemHash: p},
		{ID: "c", SemHash: 0}, // alone → below floor → no cluster
	}
	got, err := c.Cluster(context.Background(), inputs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 above-floor cluster, got %d (%+v)", len(got), got)
	}
	if got[0].ID != ClusterID(p, 4) || len(got[0].Members) != 2 {
		t.Fatalf("cluster id/members wrong: %+v", got[0])
	}
	if c.NeedsVectors() {
		t.Fatal("SimHashClusterer must not need vectors")
	}
}
