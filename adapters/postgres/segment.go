package postgres

import (
	"encoding/binary"
	"fmt"
)

// segEntry is one sealed update: its log seq and the raw update_data bytes
// (JSON-encoded []crdt.ChangeRecord, stored verbatim — never re-encoded).
type segEntry struct {
	Seq  int64
	Data []byte
}

// encodeSegment serializes entries as length-prefixed frames:
// [8-byte big-endian seq][4-byte big-endian len][len bytes] per entry.
// Deterministic for a given input order.
func encodeSegment(entries []segEntry) []byte {
	size := 0
	for _, e := range entries {
		size += 12 + len(e.Data)
	}
	buf := make([]byte, 0, size)
	var hdr [12]byte
	for _, e := range entries {
		binary.BigEndian.PutUint64(hdr[0:8], uint64(e.Seq)) // #nosec G115 -- seqs are bigserial, below int64 max
		binary.BigEndian.PutUint32(hdr[8:12], uint32(len(e.Data)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, e.Data...)
	}
	return buf
}

// decodeSegment reverses encodeSegment. A truncated frame is an error.
func decodeSegment(b []byte) ([]segEntry, error) {
	var out []segEntry
	for len(b) > 0 {
		if len(b) < 12 {
			return nil, fmt.Errorf("fabriq: truncated segment header (%d bytes left)", len(b))
		}
		seq := int64(binary.BigEndian.Uint64(b[0:8])) // #nosec G115
		n := int(binary.BigEndian.Uint32(b[8:12]))
		b = b[12:]
		if len(b) < n {
			return nil, fmt.Errorf("fabriq: truncated segment body (want %d, have %d)", n, len(b))
		}
		data := make([]byte, n)
		copy(data, b[:n])
		out = append(out, segEntry{Seq: seq, Data: data})
		b = b[n:]
	}
	return out, nil
}

// segmentKey is the deterministic blob key for a doc's sealed seq range.
// The blob store partitions by the context tenant, so no tenant is embedded.
func segmentKey(docID string, seqLo, seqHi int64) string {
	return fmt.Sprintf("crdt/%s/seg/%d-%d", docID, seqLo, seqHi)
}
