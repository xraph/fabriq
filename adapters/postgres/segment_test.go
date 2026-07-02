package postgres

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeSegmentRoundTrip(t *testing.T) {
	in := []segEntry{
		{Seq: 1, Data: []byte(`[{"field":"a"}]`)},
		{Seq: 2, Data: []byte(`[{"field":"b"},{"field":"c"}]`)},
		{Seq: 7, Data: []byte(`[]`)},
	}
	out, err := decodeSegment(encodeSegment(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len = %d, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Seq != in[i].Seq || !bytes.Equal(out[i].Data, in[i].Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, out[i], in[i])
		}
	}
}

func TestDecodeSegmentTruncated(t *testing.T) {
	full := encodeSegment([]segEntry{{Seq: 1, Data: []byte("xyz")}})
	if _, err := decodeSegment(full[:len(full)-1]); err == nil {
		t.Fatal("want error on truncated segment, got nil")
	}
}

func TestSegmentKeyDeterministic(t *testing.T) {
	k := segmentKey("page/01H", 5, 12)
	if k != "crdt/page/01H/seg/5-12" {
		t.Fatalf("segmentKey = %q", k)
	}
}
