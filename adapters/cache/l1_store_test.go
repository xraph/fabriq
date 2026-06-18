package cache

import (
	"testing"
	"time"
)

func TestL1Store_PutGetDel(t *testing.T) {
	s := newL1Store(8, time.Minute)
	if _, ok := s.get("k"); ok {
		t.Fatal("empty store must miss")
	}
	s.put("k", []byte("v"))
	v, ok := s.get("k")
	if !ok || string(v) != "v" {
		t.Fatalf("get after put: v=%q ok=%v", v, ok)
	}
	s.del("k")
	if _, ok := s.get("k"); ok {
		t.Fatal("get after del must miss")
	}
}

func TestL1Store_BoundedEviction(t *testing.T) {
	s := newL1Store(2, time.Minute) // capacity 2
	s.put("a", []byte("1"))
	s.put("b", []byte("2"))
	s.put("c", []byte("3")) // evicts the LRU entry (a)
	if _, ok := s.get("a"); ok {
		t.Fatal("a should have been evicted by capacity bound")
	}
	if _, ok := s.get("c"); !ok {
		t.Fatal("c should be present")
	}
}
