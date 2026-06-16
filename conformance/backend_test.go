package conformance

import "testing"

func TestCapabilitySet(t *testing.T) {
	set := CapabilitySet{CapRawSQL: true}
	if !set.Has(CapRawSQL) {
		t.Fatal("Has(CapRawSQL) = false, want true")
	}
	if set.Has(CapBucketedAgg) {
		t.Fatal("Has(CapBucketedAgg) = true, want false")
	}
	miss := set.missing([]Capability{CapRawSQL, CapBucketedAgg, CapPersistence})
	if len(miss) != 2 || miss[0] != CapBucketedAgg || miss[1] != CapPersistence {
		t.Fatalf("missing = %v, want [%s %s]", miss, CapBucketedAgg, CapPersistence)
	}
	if m := set.missing([]Capability{CapRawSQL}); len(m) != 0 {
		t.Fatalf("missing(present) = %v, want empty", m)
	}
}
