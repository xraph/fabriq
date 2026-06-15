package match

import "github.com/xraph/fabriq/core/query"

// evalSetString handles the set (in/notin) and string-pattern (like/ilike)
// operators. Defined in its own file because Task 3 fleshes out the matching
// logic; Task 2 lands the scalar evaluator with this returning false.
func evalSetString(c query.Cond, v any) bool {
	return false
}
