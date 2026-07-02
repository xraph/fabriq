package document

import (
	"encoding/json"
	"testing"
)

func TestHistoryUpdateShape(t *testing.T) {
	h := HistoryUpdate{Seq: 3, Update: json.RawMessage(`[{"field":"x"}]`)}
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	// camelCase JSON tags: "seq" + "update".
	if string(b) != `{"seq":3,"update":[{"field":"x"}]}` {
		t.Fatalf("marshal = %s", b)
	}
}

// staticHistoryReaderCheck fails to compile if HistoryReader drifts.
var _ = func(hr HistoryReader) {}
