package match

// Order returns -1/0/1 for order-comparable values; unordered pairs return 0.
// It exposes the package's internal comparison so the livequery package can
// order cursors with the exact same semantics the predicate evaluator uses.
func Order(a, b any) int {
	c, ok := compare(a, b)
	if !ok {
		return 0
	}
	return c
}
