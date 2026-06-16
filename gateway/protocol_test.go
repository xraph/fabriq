package gateway

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
)

func TestFrameOf_MapsDeltaWithStringOp(t *testing.T) {
	d := livequery.LiveDelta{
		Op:       livequery.OpEnter,
		AggID:    "a1",
		Version:  7,
		Row:      json.RawMessage(`{"name":"x"}`),
		OldIndex: -1,
		NewIndex: 2,
		Cursor:   livequery.Cursor{Values: []any{"x"}},
		StreamID: "evt-9",
	}
	f := frameOf(d)
	if f.Op != "enter" {
		t.Fatalf("op = %q, want enter", f.Op)
	}
	if f.AggID != "a1" || f.Version != 7 || f.NewIndex != 2 || f.OldIndex != -1 || f.StreamID != "evt-9" {
		t.Fatalf("unexpected frame: %+v", f)
	}

	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back["op"] != "enter" {
		t.Fatalf("wire op = %v, want string \"enter\"", back["op"])
	}
	if _, ok := back["at"]; ok {
		t.Fatalf("frame must not carry server-only 'at'")
	}
}

func TestClientCommand_RoundTrip(t *testing.T) {
	in := `{"action":"reanchor","cursor":{"values":["z",3]},"limit":50}`
	var c ClientCommand
	if err := json.Unmarshal([]byte(in), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.Action != ActionReanchor {
		t.Fatalf("action = %q", c.Action)
	}
	if c.Cursor == nil || len(c.Cursor.Values) != 2 || c.Limit != 50 {
		t.Fatalf("unexpected command: %+v", c)
	}
}
