package document

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSegmentInfoJSONTags(t *testing.T) {
	s := SegmentInfo{SegSeq: 1, SeqLo: 1, SeqHi: 64, UpdateCount: 64, ByteSize: 8192, At: time.Unix(0, 0).UTC()}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	want := `{"segSeq":1,"seqLo":1,"seqHi":64,"updateCount":64,"byteSize":8192,"at":"1970-01-01T00:00:00Z"}`
	if got != want {
		t.Fatalf("marshal = %s\nwant %s", got, want)
	}
}

// Compile-time capability assertions.
var _ = func(sl SegmentLister, hp HistoryPurger) {}
