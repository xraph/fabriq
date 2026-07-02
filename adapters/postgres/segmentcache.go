package postgres

import (
	"container/list"
	"sync"

	"github.com/xraph/fabriq/core/document"
)

// segmentCache is a bounded LRU of decoded history segments, keyed by blob key.
// A sealed segment is immutable, so a cached value never goes stale.
type segmentCache struct {
	mu   sync.Mutex
	max  int
	ll   *list.List // front = most recently used
	item map[string]*list.Element
}

type segCacheEntry struct {
	key string
	val []document.HistoryUpdate
}

func newSegmentCache(maxEntries int) *segmentCache {
	if maxEntries <= 0 {
		maxEntries = 128
	}
	return &segmentCache{max: maxEntries, ll: list.New(), item: map[string]*list.Element{}}
}

func (c *segmentCache) get(key string) ([]document.HistoryUpdate, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.item[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	entry, _ := el.Value.(*segCacheEntry)
	return entry.val, true
}

func (c *segmentCache) put(key string, v []document.HistoryUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.item[key]; ok {
		entry, _ := el.Value.(*segCacheEntry)
		entry.val = v
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&segCacheEntry{key: key, val: v})
	c.item[key] = el
	for c.ll.Len() > c.max {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.ll.Remove(back)
		entry, _ := back.Value.(*segCacheEntry)
		delete(c.item, entry.key)
	}
}
