package match

import "time"

// equal reports value equality with numeric cross-type coercion (JSON numbers
// arrive as float64; columns may be int64). Strings/bools compare directly.
func equal(a, b any) bool {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return af == bf
		}
	}
	if at, aok := a.(time.Time); aok {
		if bt, bok := toTime(b); bok {
			return at.Equal(bt)
		}
	}
	return a == b
}

// compare returns -1/0/1 and ok=false when the pair is not order-comparable.
func compare(a, b any) (int, bool) {
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if at, aok := a.(time.Time); aok {
		if bt, bok := toTime(b); bok {
			switch {
			case at.Before(bt):
				return -1, true
			case at.After(bt):
				return 1, true
			default:
				return 0, true
			}
		}
	}
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			switch {
			case as < bs:
				return -1, true
			case as > bs:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

func toTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case string:
		if p, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return p, true
		}
	}
	return time.Time{}, false
}
