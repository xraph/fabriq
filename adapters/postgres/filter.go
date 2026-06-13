package postgres

import (
	"fmt"

	"github.com/xraph/fabriq/core/query"
)

// sqlOp maps an engine-neutral comparison operator to its SQL token. Only
// the binary comparison ops appear here; IN/LIKE/null are special-cased in
// condSQL.
var sqlOp = map[query.Op]string{
	query.OpEq:    "=",
	query.OpNe:    "!=",
	query.OpGt:    ">",
	query.OpGte:   ">=",
	query.OpLt:    "<",
	query.OpLte:   "<=",
	query.OpLike:  "LIKE",
	query.OpILike: "ILIKE",
}

// condSQL renders one engine-neutral condition into a grove WHERE fragment
// ("?" placeholders, which grove renumbers) plus its bound args. The
// column is the only interpolated token and is quoted; values are always
// parameters. OR groups recurse into a parenthesised disjunction.
func condSQL(c query.Cond) (frag string, args []any, err error) {
	if c.IsGroup() {
		frag := ""
		var args []any
		for i, sub := range c.Or {
			f, a, err := condSQL(sub)
			if err != nil {
				return "", nil, err
			}
			if i > 0 {
				frag += " OR "
			}
			frag += f
			args = append(args, a...)
		}
		return "(" + frag + ")", args, nil
	}

	col := quoteIdent(c.Column)
	switch c.Op {
	case query.OpEq, query.OpNe, query.OpGt, query.OpGte, query.OpLt, query.OpLte, query.OpLike, query.OpILike:
		return fmt.Sprintf("%s %s ?", col, sqlOp[c.Op]), []any{c.Value}, nil
	case query.OpIn:
		return fmt.Sprintf("%s = ANY(?)", col), []any{c.Value}, nil
	case query.OpNotIn:
		return fmt.Sprintf("NOT (%s = ANY(?))", col), []any{c.Value}, nil
	case query.OpIsNull:
		return fmt.Sprintf("%s IS NULL", col), nil, nil
	case query.OpIsNotNull:
		return fmt.Sprintf("%s IS NOT NULL", col), nil, nil
	default:
		return "", nil, fmt.Errorf("fabriq: unsupported filter operator %q", c.Op)
	}
}
