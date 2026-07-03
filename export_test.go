package fabriq

import "context"

// WithFsMoveBarrier installs a test-only rendezvous invoked after MoveNode's
// pre-command ancestry guard and before the command executes, so external
// tests can force the interleaving of two concurrent moves.
func WithFsMoveBarrier(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, fsMoveBarrierKey{}, fn)
}
