package document

import (
	"encoding/json"

	"github.com/xraph/grove/crdt"
)

// ProjectValues projects a merged CRDT state onto column-keyed values —
// the single projection every document store (postgres adapter, fakes)
// materializes with: counters resolve to their total, sets and lists to
// element arrays, text to its visible string, lww/document to the stored
// value.
func ProjectValues(state *crdt.State) map[string]any {
	vals := make(map[string]any, len(state.Fields))
	for field, fs := range state.Fields {
		if fs == nil {
			continue
		}
		switch fs.Type {
		case crdt.TypeCounter:
			if fs.CounterState != nil {
				vals[field] = fs.CounterState.Value()
			}
		case crdt.TypeSet:
			if fs.SetState != nil {
				vals[field] = decodeElements(fs.SetState.Elements())
			}
		case crdt.TypeList:
			if fs.ListState != nil {
				vals[field] = decodeElements(fs.ListState.Elements())
			}
		case crdt.TypeText:
			if fs.TextState != nil {
				vals[field] = fs.TextState.Value()
			}
		default:
			if len(fs.Value) == 0 {
				continue
			}
			var v any
			if err := json.Unmarshal(fs.Value, &v); err == nil {
				vals[field] = v
			}
		}
	}
	return vals
}

func decodeElements(elements []json.RawMessage) []any {
	out := make([]any, 0, len(elements))
	for _, el := range elements {
		var v any
		if err := json.Unmarshal(el, &v); err == nil {
			out = append(out, v)
		}
	}
	return out
}
