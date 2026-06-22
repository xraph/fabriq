package agent

import (
	"context"
	"sort"
)

// ClusterInput is one item presented to a Clusterer: its node id, its SemHash
// (for hash-based clustering), and optionally its embedding Vector (populated by
// the Distiller only when the Clusterer NeedsVectors()).
type ClusterInput struct {
	ID      string
	SemHash uint64
	Vector  []float32
}

// Cluster is one emergent cluster: a node id (which MUST be a cluster-node id —
// see ClusterID, so the rest of the tree treats it as a cluster) and its members.
type Cluster struct {
	ID      string
	Members []string
}

// Clusterer groups L0 nodes into above-noise-floor clusters. The default is the
// in-core multi-probe SimHashClusterer; a host may inject a vector-based one.
//
// Coverage note: recall's cluster-coverage pruning (digestCovers / digestKind ==
// KindClusterNode) is effective ONLY for SimHash-prefix-shaped cluster ids
// produced by the in-core SimHashClusterer. A vector-based Clusterer (e.g.
// forgeext/gmmclusterer) derives its cluster ids from centroid hashes that share
// the same wire format (ClusterID) but live in a different keyspace, so
// ParseClusterID may decode them yet the prefix will rarely match an entity's
// Bucket. This is a safe no-op — it under-prunes (leaves a few redundant entity
// rows in the recall window) but NEVER false-prunes (never drops a relevant
// entity). No correctness fix is required; the note is here so maintainers
// understand the pruning asymmetry when wiring a custom Clusterer.
type Clusterer interface {
	Cluster(ctx context.Context, inputs []ClusterInput, noiseFloor int) ([]Cluster, error)
	NeedsVectors() bool
}

// SimHashClusterer is the default, deterministic, O(1)-assignment clusterer:
// primary bucket = top-`bits` SimHash prefix; a bucket clears the floor on its
// PRIMARY count; with probeRadius>0 a node also joins any already-above-floor
// cluster whose prefix is within Hamming probeRadius (probes never create a
// cluster). Member lists and cluster order are sorted (deterministic).
type SimHashClusterer struct {
	bits        int
	probeRadius int
}

// NewSimHashClusterer builds the default clusterer from the digest config.
func NewSimHashClusterer(bits, probeRadius int) *SimHashClusterer {
	return &SimHashClusterer{bits: bits, probeRadius: probeRadius}
}

func (c *SimHashClusterer) NeedsVectors() bool { return false }

func (c *SimHashClusterer) Cluster(_ context.Context, inputs []ClusterInput, noiseFloor int) ([]Cluster, error) {
	buckets := map[string][]string{}
	for _, in := range inputs {
		cid := ClusterID(ClusterPrefix(in.SemHash, c.bits), c.bits)
		buckets[cid] = append(buckets[cid], in.ID)
	}
	above := map[string]bool{}
	for cid, members := range buckets {
		if NoiseFloorMet(len(members), noiseFloor) {
			above[cid] = true
		}
	}
	effective := map[string]map[string]bool{}
	addEff := func(cid, mid string) {
		if effective[cid] == nil {
			effective[cid] = map[string]bool{}
		}
		effective[cid][mid] = true
	}
	for cid := range above {
		for _, mid := range buckets[cid] {
			addEff(cid, mid)
		}
	}
	if c.probeRadius > 0 {
		for _, in := range inputs {
			prefix := ClusterPrefix(in.SemHash, c.bits)
			for _, pp := range probePrefixes(prefix, c.bits, c.probeRadius) {
				cid := ClusterID(pp, c.bits)
				if above[cid] {
					addEff(cid, in.ID)
				}
			}
		}
	}
	cids := make([]string, 0, len(above))
	for cid := range above {
		cids = append(cids, cid)
	}
	sort.Strings(cids)
	out := make([]Cluster, 0, len(cids))
	for _, cid := range cids {
		mids := make([]string, 0, len(effective[cid]))
		for mid := range effective[cid] {
			mids = append(mids, mid)
		}
		sort.Strings(mids)
		out = append(out, Cluster{ID: cid, Members: mids})
	}
	return out, nil
}
