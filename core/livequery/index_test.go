package livequery

import (
	"sort"
	"testing"

	"github.com/xraph/fabriq/core/query"
)

func candidateList(ix *predicateIndex, row map[string]any) []string {
	set := ix.Candidates(row)
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func TestPredicateIndex_EqualityCandidates(t *testing.T) {
	ix := newPredicateIndex()
	ix.Add("a", query.Where{query.Eq("site_id", "S1"), query.Eq("status", "active")})
	ix.Add("b", query.Where{query.Eq("site_id", "S1")})
	ix.Add("c", query.Where{query.In("kind", []string{"pump", "valve"})})
	ix.Add("d", query.Where{query.Gt("temp", 50.0)}) // no equality → residual

	// S1 + active row: a (both eq), b (site), d (residual). NOT c (kind absent).
	got := candidateList(ix, map[string]any{"site_id": "S1", "status": "active"})
	want := []string{"a", "b", "d"}
	if !eqStrs(got, want) {
		t.Fatalf("S1+active candidates = %v, want %v", got, want)
	}

	// S1 + idle: a excluded (status mismatch), b included, d residual.
	got = candidateList(ix, map[string]any{"site_id": "S1", "status": "idle"})
	if !eqStrs(got, []string{"b", "d"}) {
		t.Fatalf("S1+idle candidates = %v, want [b d]", got)
	}

	// kind=pump: c (in-set), d residual. a/b need site_id (absent) → excluded.
	got = candidateList(ix, map[string]any{"kind": "pump"})
	if !eqStrs(got, []string{"c", "d"}) {
		t.Fatalf("kind=pump candidates = %v, want [c d]", got)
	}

	// kind=motor: only residual d.
	got = candidateList(ix, map[string]any{"kind": "motor"})
	if !eqStrs(got, []string{"d"}) {
		t.Fatalf("kind=motor candidates = %v, want [d]", got)
	}
}

func TestPredicateIndex_NumericNormalization(t *testing.T) {
	ix := newPredicateIndex()
	ix.Add("q", query.Where{query.Eq("qty", 3)}) // int constraint
	// JSON row carries qty as float64; the index must still match.
	if !ix.Candidates(map[string]any{"qty": 3.0})["q"] {
		t.Fatal("int constraint must match float64 row value (normalized)")
	}
	if ix.Candidates(map[string]any{"qty": 4.0})["q"] {
		t.Fatal("qty=4 must not be a candidate for qty=3")
	}
}

func TestPredicateIndex_OrIsResidual(t *testing.T) {
	ix := newPredicateIndex()
	// Top-level Eq is indexable; the Or group is not, but the Eq still gates.
	ix.Add("x", query.Where{query.Eq("site_id", "S1"), query.Or(query.Eq("a", 1), query.Eq("b", 2))})
	// A pure-Or query has no top-level equality → residual → always a candidate.
	ix.Add("y", query.Where{query.Or(query.Eq("a", 1), query.Eq("b", 2))})

	got := candidateList(ix, map[string]any{"site_id": "S1"})
	if !eqStrs(got, []string{"x", "y"}) {
		t.Fatalf("S1 candidates = %v, want [x y]", got)
	}
	got = candidateList(ix, map[string]any{"site_id": "S2"})
	if !eqStrs(got, []string{"y"}) {
		t.Fatalf("S2 candidates = %v, want [y] (x gated out by site_id)", got)
	}
}

func TestPredicateIndex_Remove(t *testing.T) {
	ix := newPredicateIndex()
	ix.Add("a", query.Where{query.Eq("site_id", "S1")})
	ix.Add("b", query.Where{query.Gt("temp", 1.0)})
	ix.Remove("a")
	ix.Remove("b")
	if len(ix.Candidates(map[string]any{"site_id": "S1"})) != 0 {
		t.Fatal("removed subs must not appear as candidates")
	}
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
