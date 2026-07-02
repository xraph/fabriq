package registry

import (
	"fmt"
	"math"
	"time"
)

// String renders the column type name used in validation error messages.
func (t ColumnType) String() string {
	switch t {
	case ColText:
		return "text"
	case ColInt:
		return "int"
	case ColFloat:
		return "float"
	case ColBool:
		return "bool"
	case ColTime:
		return "time"
	case ColJSON:
		return "json"
	}
	return fmt.Sprintf("ColumnType(%d)", int(t))
}

// CoerceToColumn validates v against the declared column type t and returns the
// canonical Go value for that type. A nil v passes through unchanged — column
// nullability is enforced separately by the required-field check. It returns an
// error when v cannot represent t.
//
// Coercion is lenient where it is safe and lossless: JSON numbers decode to
// float64, so ColInt accepts any integral float and ColFloat accepts any
// integer; ColTime accepts an RFC3339 string. ColJSON accepts any value.
func CoerceToColumn(t ColumnType, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case ColText:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return nil, typeErr(t, v)
	case ColInt:
		switch n := v.(type) {
		case int:
			return int64(n), nil
		case int8:
			return int64(n), nil
		case int16:
			return int64(n), nil
		case int32:
			return int64(n), nil
		case int64:
			return n, nil
		case uint:
			return int64(n), nil // #nosec G115 -- coerced DB int values fit in int64
		case uint8:
			return int64(n), nil
		case uint16:
			return int64(n), nil
		case uint32:
			return int64(n), nil
		case uint64:
			if n > math.MaxInt64 {
				return nil, fmt.Errorf("expects int, got uint64 %d overflowing int64", n)
			}
			return int64(n), nil
		case float32:
			return floatToInt(float64(n))
		case float64:
			return floatToInt(n)
		}
		return nil, typeErr(t, v)
	case ColFloat:
		switch n := v.(type) {
		case float32:
			return float64(n), nil
		case float64:
			return n, nil
		case int:
			return float64(n), nil
		case int8:
			return float64(n), nil
		case int16:
			return float64(n), nil
		case int32:
			return float64(n), nil
		case int64:
			return float64(n), nil
		case uint:
			return float64(n), nil
		case uint8:
			return float64(n), nil
		case uint16:
			return float64(n), nil
		case uint32:
			return float64(n), nil
		case uint64:
			return float64(n), nil
		}
		return nil, typeErr(t, v)
	case ColBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, typeErr(t, v)
	case ColTime:
		switch tv := v.(type) {
		case time.Time:
			return tv, nil
		case string:
			parsed, err := time.Parse(time.RFC3339, tv)
			if err != nil {
				return nil, fmt.Errorf("expects time (RFC3339), got %q", tv)
			}
			return parsed, nil
		}
		return nil, typeErr(t, v)
	case ColJSON:
		return v, nil
	}
	return nil, fmt.Errorf("unknown column type %d", t)
}

func floatToInt(f float64) (any, error) {
	if f != math.Trunc(f) {
		return nil, fmt.Errorf("expects int, got non-integral float %v", f)
	}
	return int64(f), nil
}

func typeErr(t ColumnType, v any) error {
	return fmt.Errorf("expects %s, got %T", t, v)
}
