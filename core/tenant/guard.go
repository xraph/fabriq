package tenant

import "context"

// Require asserts that ctx is tenant-stamped and returns the tenant id.
// It is FromContext under the guard's name: fabric entry points call
// Require so call sites read as the structural assertions they are.
func Require(ctx context.Context) (string, error) {
	return FromContext(ctx)
}
