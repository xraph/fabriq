package agent

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
)

// NewSemPlanes builds 64 fixed random hyperplanes of the given dimensionality,
// deterministically from seed. Persisting planes is equivalent to fixing the
// seed, so callers get stable SemHashes across processes by passing a constant.
func NewSemPlanes(dims int, seed int64) [64][]float32 {
	r := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic PRNG by design: a fixed seed yields stable hyperplanes across processes; not security-sensitive
	var planes [64][]float32
	for i := range planes {
		p := make([]float32, dims)
		for j := range p {
			p[j] = float32(r.NormFloat64())
		}
		planes[i] = p
	}
	return planes
}

// SemHash is a 64-bit SimHash (LSH): bit i = sign(embedding · planeᵢ). Cosine-
// close embeddings produce Hamming-close hashes. A zero/empty embedding hashes
// to 0. Planes shorter than the embedding compare over the shared prefix.
func SemHash(embedding []float32, planes [64][]float32) uint64 {
	var h uint64
	for i := 0; i < 64; i++ {
		p := planes[i]
		n := len(p)
		if len(embedding) < n {
			n = len(embedding)
		}
		var dot float64
		for j := 0; j < n; j++ {
			dot += float64(embedding[j]) * float64(p[j])
		}
		if dot >= 0 && !math.IsNaN(dot) {
			h |= uint64(1) << uint(i)
		}
	}
	return h
}

// HammingDistance counts differing bits.
func HammingDistance(a, b uint64) int { return popcount(a ^ b) }

// HammingClose reports whether a and b differ in at most maxBits bits.
func HammingClose(a, b uint64, maxBits int) bool { return HammingDistance(a, b) <= maxBits }

func popcount(x uint64) int {
	c := 0
	for x != 0 {
		x &= x - 1
		c++
	}
	return c
}

// FormatSemHash renders a SemHash as 16 lowercase hex chars (JSON/precision-safe).
func FormatSemHash(h uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], h)
	return hex.EncodeToString(b[:])
}

// ParseSemHash parses the 16-hex form produced by FormatSemHash.
func ParseSemHash(s string) (uint64, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 8 {
		return 0, fmt.Errorf("agent: invalid sem hash %q", s)
	}
	return binary.BigEndian.Uint64(b), nil
}

// ClusterPrefix keeps the top p bits of h (the bucket key); the rest are zeroed.
func ClusterPrefix(h uint64, p int) uint64 {
	if p <= 0 {
		return 0
	}
	if p >= 64 {
		return h
	}
	return (h >> uint(64-p)) << uint(64-p)
}
