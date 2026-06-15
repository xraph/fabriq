package livequery

import "github.com/xraph/fabriq/core/livequery/match"

// SortKeyOf builds the keyset cursor for a row: one value per sort key, then
// the id as the final unique tiebreak (so (Sort…, id) is a total order).
func SortKeyOf(row map[string]any, sort []SortKey, id string) Cursor {
	vals := make([]any, 0, len(sort)+1)
	for _, s := range sort {
		vals = append(vals, row[s.Column])
	}
	vals = append(vals, id)
	return Cursor{Values: vals}
}

// CompareCursors returns -1/0/1 ordering a before/equal/after b under sort,
// honoring per-key Desc and using the trailing id value as the final ASC
// tiebreak. Cursors must come from SortKeyOf with the same sort spec.
func CompareCursors(a, b Cursor, sort []SortKey) int {
	for i, s := range sort {
		c := match.Order(a.Values[i], b.Values[i])
		if s.Desc {
			c = -c
		}
		if c != 0 {
			return c
		}
	}
	// trailing id, always ascending
	ai, _ := a.Values[len(a.Values)-1].(string)
	bi, _ := b.Values[len(b.Values)-1].(string)
	switch {
	case ai < bi:
		return -1
	case ai > bi:
		return 1
	default:
		return 0
	}
}
