package agent

import (
	"context"
	"hash/fnv"
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestProbePrefixes_Radius1 enumerates the single-bit-flip neighbors of a prefix
// over the top `bits` bits.
func TestProbePrefixes_Radius1(t *testing.T) {
	// top 4 bits all set: 1111 0000... ; flipping each of the top 4 bits yields 4 neighbors.
	prefix := ClusterPrefix(^uint64(0), 4) // top 4 bits = 1
	got := probePrefixes(prefix, 4, 1)
	if len(got) != 4 {
		t.Fatalf("radius-1 over 4 bits = 4 neighbors, got %d (%v)", len(got), got)
	}
	for _, n := range got {
		if HammingDistance(prefix, n) != 1 {
			t.Fatalf("neighbor %x is not Hamming-1 from %x", n, prefix)
		}
	}
}

// TestProbePrefixes_DisabledAndBounds covers radius 0 (disabled) and radius>bits.
func TestProbePrefixes_DisabledAndBounds(t *testing.T) {
	if got := probePrefixes(0, 4, 0); got != nil {
		t.Fatalf("radius 0 must return nil, got %v", got)
	}
	// radius clamps to bits; size = C(4,1)+C(4,2)+C(4,3)+C(4,4) = 4+6+4+1 = 15
	if got := probePrefixes(0, 4, 99); len(got) != 15 {
		t.Fatalf("radius>bits clamps to bits; want 15 neighbors, got %d", len(got))
	}
}

// variedEmbedder maps text → a deterministic 8-dim unit-ish vector so distinct
// texts get distinct SemHashes (the constant stubEmbedder makes all SemHashes
// equal, which can't exercise clustering boundaries).
type variedEmbedder struct{ dims int }

func (e variedEmbedder) Dims() int { return e.dims }
func (e variedEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		h := fnv.New64a()
		_, _ = h.Write([]byte(t))
		seed := h.Sum64()
		v := make([]float32, e.dims)
		for j := range v {
			seed = seed*6364136223846793005 + 1442695040888963407
			v[j] = float32(int64(seed>>11)) / float32(1<<52)
		}
		out[i] = v
	}
	return out, nil
}

func newProbeDistiller(t *testing.T, r *registry.Registry, cas *fabriqtest.FakeCAS, probe int) *Distiller {
	t.Helper()
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	d, err := NewDistiller(fab, r, variedEmbedder{dims: 8}, &fakeSummarizer{}, nil, cas,
		DistillConfig{VectorDims: 8, RecipeVersion: "v1", ClusterBits: 4, NoiseFloor: 2, ProbeRadius: probe})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestMultiProbe_NeverCreatesBelowFloorCluster asserts probing only ADDS members
// to clusters that already cleared the floor on their primary members — a probe
// never materializes a below-floor cluster node.
func TestMultiProbe_NeverCreatesBelowFloorCluster(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d := newProbeDistiller(t, r, cas, 1)
	ctx := testCtx(t)

	// Distill several notes; with ProbeRadius=1 some may join neighbor clusters,
	// but every cluster node that exists must have >= NoiseFloor PRIMARY members.
	for i, body := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		id := string(rune('a' + i))
		if _, err := d.DistillL0(ctx, "note", id, map[string]any{"id": id, "title": id, "body": body, "site_id": "s1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	// Every cluster node present must trace to a primary bucket of >= NoiseFloor.
	l1s, err := d.listNodes(ctx, LevelScope)
	if err != nil {
		t.Fatal(err)
	}
	l0s, err := d.listNodes(ctx, LevelEntity)
	if err != nil {
		t.Fatal(err)
	}
	primary := map[string]int{}
	for _, n := range l0s {
		cid := ClusterID(ClusterPrefix(parseSemOrZero(n.SemHash), 4), 4)
		primary[cid]++
	}
	for _, n := range l1s {
		if isClusterID(n.ID) && primary[n.ID] < 2 {
			t.Fatalf("cluster %s exists with primary count %d < NoiseFloor 2", n.ID, primary[n.ID])
		}
	}
}
