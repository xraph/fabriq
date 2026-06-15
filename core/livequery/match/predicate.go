// Package match compiles a query.Where (fabriq's engine-neutral filter AST)
// into a Go predicate evaluated against a column-keyed map. It is the hot-path
// twin of the SQL the postgres adapter generates from the same Where, so a
// live query's membership test and its snapshot/refill SQL share one source of
// truth. The matchtest parity suite asserts Go-eval ≡ Postgres WHERE.
package match

import (
	"fmt"

	"github.com/xraph/fabriq/core/query"
)

// Predicate evaluates a compiled filter against a column-keyed row.
type Predicate struct {
	conds []query.Cond
}

// Compile validates the operator vocabulary (column validation belongs
// upstream in query.ValidateConds) and returns an evaluator over the
// conjunction of conds.
func Compile(w query.Where) (Predicate, error) {
	for _, c := range w {
		if err := checkOp(c); err != nil {
			return Predicate{}, err
		}
	}
	return Predicate{conds: w}, nil
}

func checkOp(c query.Cond) error {
	if c.IsGroup() {
		for _, sub := range c.Or {
			if err := checkOp(sub); err != nil {
				return err
			}
		}
		return nil
	}
	if !c.Op.IsSet() {
		return fmt.Errorf("fabriq: match: unknown operator %q", c.Op)
	}
	return nil
}

// Eval reports whether row satisfies every top-level condition (AND).
func (p Predicate) Eval(row map[string]any) bool {
	for _, c := range p.conds {
		if !evalCond(c, row) {
			return false
		}
	}
	return true
}

func evalCond(c query.Cond, row map[string]any) bool {
	if c.IsGroup() {
		for _, sub := range c.Or {
			if evalCond(sub, row) {
				return true
			}
		}
		return false
	}
	v, present := row[c.Column]
	switch c.Op {
	case query.OpIsNull:
		return !present || v == nil
	case query.OpIsNotNull:
		return present && v != nil
	}
	if !present || v == nil {
		return false // NULL compares not-true to everything (SQL three-valued logic)
	}
	switch c.Op {
	case query.OpEq:
		return equal(v, c.Value)
	case query.OpNe:
		return !equal(v, c.Value)
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		cmp, ok := compare(v, c.Value)
		if !ok {
			return false
		}
		switch c.Op {
		case query.OpGt:
			return cmp > 0
		case query.OpGte:
			return cmp >= 0
		case query.OpLt:
			return cmp < 0
		default:
			return cmp <= 0
		}
	}
	return evalSetString(c, v)
}
