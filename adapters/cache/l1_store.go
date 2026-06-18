package cache

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// l1Store is a bounded, TTL'd, concurrency-safe byte cache backing the L1 tier.
// expirable.LRU is itself safe for concurrent use.
type l1Store struct {
	lru *expirable.LRU[string, []byte]
}

// newL1Store builds a store holding at most size entries, each expiring after
// ttl (ttl <= 0 means no expiry; size <= 0 means unbounded — callers pass
// sensible bounds).
func newL1Store(size int, ttl time.Duration) *l1Store {
	return &l1Store{lru: expirable.NewLRU[string, []byte](size, nil, ttl)}
}

func (s *l1Store) get(key string) ([]byte, bool) { return s.lru.Get(key) }
func (s *l1Store) put(key string, val []byte)    { s.lru.Add(key, val) }
func (s *l1Store) del(key string)                { s.lru.Remove(key) }
