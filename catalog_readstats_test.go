package fabriq

import "testing"

func TestStores_CatalogReadStats_NilWithoutReplicas(t *testing.T) {
	s := &Stores{}
	if _, _, _, ok := s.CatalogReadStats(); ok {
		t.Fatal("no Failover ⇒ ok must be false")
	}
}
