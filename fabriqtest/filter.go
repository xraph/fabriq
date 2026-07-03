package fabriqtest

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/xraph/fabriq/core/query"
)

// parseOrderBy splits "col [DESC]" into (column, descending).
func parseOrderBy(orderBy string) (col string, desc bool) {
	parts := strings.Fields(orderBy)
	if len(parts) == 0 {
		return "", false
	}
	return parts[0], len(parts) > 1 && strings.EqualFold(parts[1], "DESC")
}

// evalConds evaluates an ANDed condition list against an in-memory row.
// It mirrors the postgres adapter's SQL semantics closely enough for unit
// tests; the integration suite is the source of truth for exact behaviour.
func evalConds(vals map[string]any, conds []query.Cond) (bool, error) {
	for _, c := range conds {
		ok, err := evalCond(vals, c)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalCond(vals map[string]any, c query.Cond) (bool, error) {
	if c.IsGroup() {
		for _, sub := range c.Or {
			ok, err := evalCond(vals, sub)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}

	v, present := vals[c.Column]
	switch c.Op {
	case query.OpIsNull:
		return !present || isNullVal(v), nil
	case query.OpIsNotNull:
		return present && !isNullVal(v), nil
	case query.OpEq:
		return valuesEqual(v, c.Value), nil
	case query.OpNe:
		return !valuesEqual(v, c.Value), nil
	case query.OpIn:
		return inSlice(v, c.Value), nil
	case query.OpNotIn:
		return !inSlice(v, c.Value), nil
	case query.OpLike:
		return likeMatch(v, c.Value, false)
	case query.OpILike:
		return likeMatch(v, c.Value, true)
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		cmp, ok := compareVals(v, c.Value)
		if !ok {
			return false, nil
		}
		switch c.Op {
		case query.OpGt:
			return cmp > 0, nil
		case query.OpGte:
			return cmp >= 0, nil
		case query.OpLt:
			return cmp < 0, nil
		default: // OpLte
			return cmp <= 0, nil
		}
	default:
		return false, fmt.Errorf("fabriq: fake List cannot evaluate operator %q", c.Op)
	}
}

// isNullVal reports whether a stored value is SQL NULL. Rows hold model
// fields boxed via reflect .Interface(), so a nil *time.Time (etc.) arrives
// as a typed-nil interface that `v == nil` misses — those columns are NULL
// on the SQL backends and must be NULL here too.
func isNullVal(v any) bool {
	if v == nil {
		return true
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// valuesEqual compares two values numeric-aware (int64 stored vs int
// literal) and falls back to deep equality.
func valuesEqual(a, b any) bool {
	if fa, oka := toFloat(a); oka {
		if fb, okb := toFloat(b); okb {
			return fa == fb
		}
	}
	return reflect.DeepEqual(a, b)
}

func inSlice(v, slice any) bool {
	rv := reflect.ValueOf(slice)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	for i := 0; i < rv.Len(); i++ {
		if valuesEqual(v, rv.Index(i).Interface()) {
			return true
		}
	}
	return false
}

// compareVals returns -1/0/1 for ordered values (numbers or strings), and
// false when they are not comparable.
func compareVals(a, b any) (int, bool) {
	if fa, oka := toFloat(a); oka {
		if fb, okb := toFloat(b); okb {
			switch {
			case fa < fb:
				return -1, true
			case fa > fb:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	sa, oka := a.(string)
	sb, okb := b.(string)
	if oka && okb {
		return strings.Compare(sa, sb), true
	}
	return 0, false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// likeMatch emulates SQL LIKE/ILIKE: % matches any run, _ matches one
// character; everything else is literal.
func likeMatch(v, pattern any, ci bool) (bool, error) {
	s, ok := v.(string)
	if !ok {
		return false, nil
	}
	p, ok := pattern.(string)
	if !ok {
		return false, fmt.Errorf("fabriq: LIKE pattern must be a string, got %T", pattern)
	}
	var b strings.Builder
	if ci {
		b.WriteString("(?i)")
	}
	b.WriteString("^")
	for _, r := range p {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false, err
	}
	return re.MatchString(s), nil
}
