package postgres

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/document"
)

func TestSegmentCacheGetPut(t *testing.T) {
	c := newSegmentCache(2)
	if _, ok := c.get("a"); ok {
		t.Fatal("empty cache returned a hit")
	}
	c.put("a", []document.HistoryUpdate{{Seq: 1, Update: json.RawMessage(`[]`)}})
	got, ok := c.get("a")
	if !ok || len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("get(a) = %+v ok=%v", got, ok)
	}
}

func TestSegmentCacheEvictsLRU(t *testing.T) {
	c := newSegmentCache(2)
	c.put("a", []document.HistoryUpdate{{Seq: 1}})
	c.put("b", []document.HistoryUpdate{{Seq: 2}})
	_, _ = c.get("a")                              // touch a → b is now least-recently-used
	c.put("c", []document.HistoryUpdate{{Seq: 3}}) // evicts b
	if _, ok := c.get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.get("a"); !ok {
		t.Fatal("a should still be present")
	}
	if _, ok := c.get("c"); !ok {
		t.Fatal("c should be present")
	}
}
