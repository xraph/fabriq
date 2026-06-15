package match

import (
	"reflect"
	"strings"

	"github.com/xraph/fabriq/core/query"
)

// evalSetString handles the set (in/notin) and string-pattern (like/ilike)
// operators against a present, non-nil value v.
func evalSetString(c query.Cond, v any) bool {
	switch c.Op {
	case query.OpIn:
		return inSlice(v, c.Value)
	case query.OpNotIn:
		return !inSlice(v, c.Value)
	case query.OpLike:
		return likeMatch(v, c.Value, false)
	case query.OpILike:
		return likeMatch(v, c.Value, true)
	}
	return false
}

func inSlice(v, set any) bool {
	rv := reflect.ValueOf(set)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	for i := 0; i < rv.Len(); i++ {
		if equal(v, rv.Index(i).Interface()) {
			return true
		}
	}
	return false
}

// likeMatch translates a SQL LIKE pattern (% and _ wildcards, no escaping in
// P1) into anchored matching. ci=true folds case (ILIKE).
func likeMatch(v, pattern any, ci bool) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	pat, ok := pattern.(string)
	if !ok {
		return false
	}
	if ci {
		s = strings.ToLower(s)
		pat = strings.ToLower(pat)
	}
	return likeAnchored(s, pat)
}

// likeAnchored implements % (any run, incl. empty) and _ (single char) against
// the whole string via two-pointer backtracking.
func likeAnchored(s, pat string) bool {
	var si, pi, star, mark int
	star = -1
	for si < len(s) {
		if pi < len(pat) && (pat[pi] == '_' || pat[pi] == s[si]) {
			si++
			pi++
		} else if pi < len(pat) && pat[pi] == '%' {
			star = pi
			mark = si
			pi++
		} else if star != -1 {
			pi = star + 1
			mark++
			si = mark
		} else {
			return false
		}
	}
	for pi < len(pat) && pat[pi] == '%' {
		pi++
	}
	return pi == len(pat)
}
