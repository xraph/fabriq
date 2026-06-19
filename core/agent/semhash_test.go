package agent

import "testing"

func TestSemHash_CloseVectorsAreHammingClose(t *testing.T) {
	planes := NewSemPlanes(8, 42)
	a := []float32{1, 0.9, 0.1, 0, -1, -0.8, 0.2, 0.3}
	bClose := []float32{0.95, 0.92, 0.08, 0.02, -0.97, -0.79, 0.18, 0.31} // ~same direction
	bFar := []float32{-1, -0.9, -0.1, 0, 1, 0.8, -0.2, -0.3}              // opposite
	ha, hc, hf := SemHash(a, planes), SemHash(bClose, planes), SemHash(bFar, planes)
	if HammingDistance(ha, hc) > HammingDistance(ha, hf) {
		t.Fatalf("close vector farther than opposite: close=%d far=%d", HammingDistance(ha, hc), HammingDistance(ha, hf))
	}
	if !HammingClose(ha, hc, 8) {
		t.Fatalf("expected close vectors within 8 bits, got %d", HammingDistance(ha, hc))
	}
}

func TestSemHash_Deterministic(t *testing.T) {
	p1, p2 := NewSemPlanes(8, 7), NewSemPlanes(8, 7)
	v := []float32{0.1, -0.2, 0.3, -0.4, 0.5, -0.6, 0.7, -0.8}
	if SemHash(v, p1) != SemHash(v, p2) {
		t.Fatal("planes from the same seed must produce identical hashes")
	}
}

func TestSemHash_HexRoundTrip(t *testing.T) {
	var h uint64 = 0xDEADBEEF12345678
	got, err := ParseSemHash(FormatSemHash(h))
	if err != nil || got != h {
		t.Fatalf("round trip failed: got=%x err=%v", got, err)
	}
	if len(FormatSemHash(0)) != 16 {
		t.Fatalf("expected 16 hex chars, got %q", FormatSemHash(0))
	}
}

func TestClusterPrefix(t *testing.T) {
	// top 4 bits of 0xF000... is 0xF, shifted to occupy those 4 high bits.
	h := uint64(0xF000000000000000)
	if ClusterPrefix(h, 4) != (uint64(0xF) << 60) {
		t.Fatalf("cluster prefix wrong: %x", ClusterPrefix(h, 4))
	}
}
