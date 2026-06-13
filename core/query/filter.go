package query

import (
	"fmt"
	"reflect"
)

// Op is the bounded, engine-neutral comparison vocabulary the
// RelationalQuerier accepts on List filters. It is deliberately a fixed
// allowlist (not arbitrary SQL): adapters map each Op to their dialect,
// columns are validated against the entity, and values travel as bound
// parameters — so a structured filter is as injection-safe as the
// equality shorthand, while covering what grove's builder expresses.
// Anything outside this vocabulary belongs in the raw Query escape hatch.
type Op string

const (
	OpEq        Op = "eq"
	OpNe        Op = "ne"
	OpGt        Op = "gt"
	OpGte       Op = "gte"
	OpLt        Op = "lt"
	OpLte       Op = "lte"
	OpIn        Op = "in"
	OpNotIn     Op = "notin"
	OpLike      Op = "like"
	OpILike     Op = "ilike"
	OpIsNull    Op = "isnull"
	OpIsNotNull Op = "isnotnull"
)

// Cond is one engine-neutral predicate on a column — or, when Or is set,
// an OR group of sub-predicates. A flat []Cond is ANDed; an Or group lets
// you express "(a OR b)" without a full expression tree. Build them with
// the constructors (Eq, In, Like, Or, …) rather than by hand.
type Cond struct {
	Column string
	Op     Op
	Value  any    // a slice for In/NotIn; ignored for IsNull/IsNotNull
	Or     []Cond // when non-empty, an OR group (Column/Op/Value ignored)
}

// Eq builds column = value.
func Eq(column string, value any) Cond { return Cond{Column: column, Op: OpEq, Value: value} }

// Ne builds column != value.
func Ne(column string, value any) Cond { return Cond{Column: column, Op: OpNe, Value: value} }

// Gt builds column > value.
func Gt(column string, value any) Cond { return Cond{Column: column, Op: OpGt, Value: value} }

// Gte builds column >= value.
func Gte(column string, value any) Cond { return Cond{Column: column, Op: OpGte, Value: value} }

// Lt builds column < value.
func Lt(column string, value any) Cond { return Cond{Column: column, Op: OpLt, Value: value} }

// Lte builds column <= value.
func Lte(column string, value any) Cond { return Cond{Column: column, Op: OpLte, Value: value} }

// In builds column IN (values...); values must be a non-empty slice.
func In(column string, values any) Cond { return Cond{Column: column, Op: OpIn, Value: values} }

// NotIn builds column NOT IN (values...); values must be a non-empty slice.
func NotIn(column string, values any) Cond { return Cond{Column: column, Op: OpNotIn, Value: values} }

// Like builds column LIKE pattern (case-sensitive; SQL % / _ wildcards).
func Like(column, pattern string) Cond { return Cond{Column: column, Op: OpLike, Value: pattern} }

// ILike builds column ILIKE pattern (case-insensitive).
func ILike(column, pattern string) Cond { return Cond{Column: column, Op: OpILike, Value: pattern} }

// IsNull builds column IS NULL.
func IsNull(column string) Cond { return Cond{Column: column, Op: OpIsNull} }

// IsNotNull builds column IS NOT NULL.
func IsNotNull(column string) Cond { return Cond{Column: column, Op: OpIsNotNull} }

// Or groups sub-conditions into a single OR predicate.
func Or(conds ...Cond) Cond { return Cond{Or: conds} }

// IsGroup reports whether the Cond is an OR group rather than a leaf.
func (c Cond) IsGroup() bool { return len(c.Or) > 0 }

// NeedsValue reports whether the operator takes a value.
func (o Op) NeedsValue() bool { return o != OpIsNull && o != OpIsNotNull }

// IsSet reports whether the operator is one of the known operators.
func (o Op) IsSet() bool {
	switch o {
	case OpEq, OpNe, OpGt, OpGte, OpLt, OpLte, OpIn, OpNotIn, OpLike, OpILike, OpIsNull, OpIsNotNull:
		return true
	default:
		return false
	}
}

// ValidateConds checks a filter tree against an entity's columns and the
// operator vocabulary. has reports whether a column exists. Adapters call
// this before translating, so the same rules guard every engine.
func ValidateConds(conds []Cond, has func(column string) bool) error {
	for _, c := range conds {
		if err := c.validate(has); err != nil {
			return err
		}
	}
	return nil
}

func (c Cond) validate(has func(string) bool) error {
	if c.IsGroup() {
		if len(c.Or) == 0 {
			return fmt.Errorf("fabriq: empty OR group")
		}
		return ValidateConds(c.Or, has)
	}
	if c.Column == "" {
		return fmt.Errorf("fabriq: filter condition with empty column")
	}
	if !has(c.Column) {
		return fmt.Errorf("fabriq: filter references unknown column %q", c.Column)
	}
	if !c.Op.IsSet() {
		return fmt.Errorf("fabriq: unknown filter operator %q on %q", c.Op, c.Column)
	}
	switch c.Op {
	case OpIn, OpNotIn:
		rv := reflect.ValueOf(c.Value)
		if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
			return fmt.Errorf("fabriq: operator %q on %q needs a slice value", c.Op, c.Column)
		}
		if rv.Len() == 0 {
			return fmt.Errorf("fabriq: operator %q on %q needs a non-empty slice", c.Op, c.Column)
		}
	case OpIsNull, OpIsNotNull:
		// no value
	default:
		if c.Value == nil {
			return fmt.Errorf("fabriq: operator %q on %q needs a value", c.Op, c.Column)
		}
	}
	return nil
}
