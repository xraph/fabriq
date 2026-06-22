// Package gmmclusterer is an optional, host-wired vector clusterer for fabriq
// context distillation. It implements agent.Clusterer over embedding vectors
// (NeedsVectors()=true) as a deterministic k-means — a pragmatic alternative to
// the in-core multi-probe SimHash default. True GMM+UMAP is a documented
// extension; this keeps core import-clean (lives in forgeext, stdlib-only).
package gmmclusterer

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sort"

	"github.com/xraph/fabriq/core/agent"
)

// Clusterer is a deterministic k-means vector clusterer.
type Clusterer struct {
	k       int
	idBits  int
	maxIter int
}

// New builds a k-means clusterer producing up to k clusters; idBits sizes the
// cluster-id prefix (mirrors the SimHash cluster id shape).
func New(k, idBits int) *Clusterer {
	if k < 1 {
		k = 1
	}
	if idBits <= 0 || idBits > 64 {
		idBits = 12
	}
	return &Clusterer{k: k, idBits: idBits, maxIter: 20}
}

func (*Clusterer) NeedsVectors() bool { return true }

func (c *Clusterer) Cluster(_ context.Context, inputs []agent.ClusterInput, noiseFloor int) ([]agent.Cluster, error) {
	// Filter to inputs that carry a vector.
	pts := make([]agent.ClusterInput, 0, len(inputs))
	for _, in := range inputs {
		if len(in.Vector) > 0 {
			pts = append(pts, in)
		}
	}
	if len(pts) == 0 {
		return nil, nil
	}
	k := c.k
	if k > len(pts) {
		k = len(pts)
	}
	dim := len(pts[0].Vector)

	// Deterministic k-means++ seeding from a fixed first index.
	centroids := make([][]float32, 0, k)
	centroids = append(centroids, cloneVec(pts[0].Vector))
	for len(centroids) < k {
		best, bestD := -1, float64(-1)
		for i := range pts {
			d := nearestDist(pts[i].Vector, centroids)
			if d > bestD {
				bestD, best = d, i
			}
		}
		if best < 0 {
			break
		}
		centroids = append(centroids, cloneVec(pts[best].Vector))
	}

	assign := make([]int, len(pts))
	for iter := 0; iter < c.maxIter; iter++ {
		changed := false
		for i := range pts {
			a := nearestIdx(pts[i].Vector, centroids)
			if a != assign[i] {
				assign[i], changed = a, true
			}
		}
		sums := make([][]float64, k)
		counts := make([]int, k)
		for j := range sums {
			sums[j] = make([]float64, dim)
		}
		for i := range pts {
			counts[assign[i]]++
			for d := 0; d < dim; d++ {
				sums[assign[i]][d] += float64(pts[i].Vector[d])
			}
		}
		for j := 0; j < k; j++ {
			if counts[j] == 0 {
				continue
			}
			for d := 0; d < dim; d++ {
				centroids[j][d] = float32(sums[j][d] / float64(counts[j]))
			}
		}
		if !changed {
			break
		}
	}

	members := make([][]string, k)
	for i := range pts {
		members[assign[i]] = append(members[assign[i]], pts[i].ID)
	}

	out := []agent.Cluster{}
	for j := 0; j < k; j++ {
		if len(members[j]) < noiseFloor {
			continue
		}
		sort.Strings(members[j])
		out = append(out, agent.Cluster{ID: c.clusterID(centroids[j]), Members: members[j]})
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out, nil
}

// clusterID derives a stable cluster-node id from the centroid (hash of its
// rounded coordinates → SimHash-shaped prefix), so it satisfies isClusterID.
func (c *Clusterer) clusterID(centroid []float32) string {
	h := fnv.New64a()
	var b [8]byte
	for _, v := range centroid {
		binary.BigEndian.PutUint64(b[:], math.Float64bits(math.Round(float64(v)*1e4)/1e4))
		_, _ = h.Write(b[:])
	}
	prefix := agent.ClusterPrefix(h.Sum64(), c.idBits)
	return agent.ClusterID(prefix, c.idBits)
}

func cloneVec(v []float32) []float32 {
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

func dist(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i] - b[i])
		s += d * d
	}
	return s
}

func nearestIdx(v []float32, cs [][]float32) int {
	best, bd := 0, math.Inf(1)
	for j := range cs {
		if d := dist(v, cs[j]); d < bd {
			bd, best = d, j
		}
	}
	return best
}

func nearestDist(v []float32, cs [][]float32) float64 {
	bd := math.Inf(1)
	for j := range cs {
		if d := dist(v, cs[j]); d < bd {
			bd = d
		}
	}
	return bd
}

var _ agent.Clusterer = (*Clusterer)(nil)
