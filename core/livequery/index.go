package livequery

import "github.com/xraph/fabriq/core/query"

// predicateIndex is the content-based candidate selector (the counting
// algorithm). It indexes each subscription's TOP-LEVEL equality constraints
// (Eq / In, ANDed — not those inside Or groups) so that a change is matched
// against only the subscriptions whose equality gates it could satisfy, instead
// of every subscription. It is a SOUND pre-filter: it never excludes a
// subscription that could match, but it may over-include (the full predicate is
// evaluated afterwards). Subscriptions with no indexable equality are residual
// and are always candidates.
//
// It is not safe for concurrent use; callers serialize access (the dispatcher
// owns it on a single goroutine).
type predicateIndex struct {
	// column -> normalized value -> set of subIDs accepting that (col,val).
	byColVal map[string]map[any]map[string]bool
	// subID -> number of distinct indexed columns it constrains.
	want map[string]int
	// subIDs with zero indexable equality constraints (always candidates).
	residual map[string]bool
}

func newPredicateIndex() *predicateIndex {
	return &predicateIndex{
		byColVal: map[string]map[any]map[string]bool{},
		want:     map[string]int{},
		residual: map[string]bool{},
	}
}

// normVal canonicalizes a value for index keying so that, e.g., an int
// constraint matches a float64 row value (JSON numbers decode to float64),
// mirroring the match evaluator's numeric coercion.
func normVal(v any) any {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	}
	return v
}

// equalityColumns returns each subscription's top-level equality constraints as
// column -> accepted normalized values. Or groups and non-equality operators
// are skipped (they fall to the full evaluator).
func equalityColumns(w query.Where) map[string][]any {
	out := map[string][]any{}
	for _, c := range w {
		if c.IsGroup() {
			continue
		}
		switch c.Op {
		case query.OpEq:
			out[c.Column] = append(out[c.Column], normVal(c.Value))
		case query.OpIn:
			for _, v := range toAnySlice(c.Value) {
				out[c.Column] = append(out[c.Column], normVal(v))
			}
		}
	}
	return out
}

// Add registers a subscription's filter in the index.
func (ix *predicateIndex) Add(id string, w query.Where) {
	cols := equalityColumns(w)
	if len(cols) == 0 {
		ix.residual[id] = true
		return
	}
	ix.want[id] = len(cols)
	for col, vals := range cols {
		byVal := ix.byColVal[col]
		if byVal == nil {
			byVal = map[any]map[string]bool{}
			ix.byColVal[col] = byVal
		}
		for _, v := range vals {
			ids := byVal[v]
			if ids == nil {
				ids = map[string]bool{}
				byVal[v] = ids
			}
			ids[id] = true
		}
	}
}

// Remove drops a subscription from the index.
func (ix *predicateIndex) Remove(id string) {
	delete(ix.residual, id)
	delete(ix.want, id)
	for col, byVal := range ix.byColVal {
		for v, ids := range byVal {
			delete(ids, id)
			if len(ids) == 0 {
				delete(byVal, v)
			}
		}
		if len(byVal) == 0 {
			delete(ix.byColVal, col)
		}
	}
}

// Candidates returns the set of subIDs whose indexed equality constraints are
// all satisfied by row, unioned with every residual subID. A nil row (a delete,
// which carries no new state) yields only the residual set; the dispatcher
// unions current members separately to catch leaves.
func (ix *predicateIndex) Candidates(row map[string]any) map[string]bool {
	out := make(map[string]bool, len(ix.residual))
	for id := range ix.residual {
		out[id] = true
	}
	if row == nil || len(ix.want) == 0 {
		return out
	}
	hits := make(map[string]int, len(ix.want))
	for col, raw := range row {
		byVal := ix.byColVal[col]
		if byVal == nil {
			continue
		}
		ids := byVal[normVal(raw)]
		for id := range ids {
			hits[id]++
		}
	}
	for id, n := range hits {
		if n >= ix.want[id] {
			out[id] = true
		}
	}
	return out
}

func toAnySlice(v any) []any {
	switch s := v.(type) {
	case []any:
		return s
	case []string:
		out := make([]any, len(s))
		for i, x := range s {
			out[i] = x
		}
		return out
	case []int:
		out := make([]any, len(s))
		for i, x := range s {
			out[i] = x
		}
		return out
	case []int64:
		out := make([]any, len(s))
		for i, x := range s {
			out[i] = x
		}
		return out
	case []float64:
		out := make([]any, len(s))
		for i, x := range s {
			out[i] = x
		}
		return out
	}
	return nil
}
