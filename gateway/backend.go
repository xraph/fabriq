package gateway

import (
	"context"

	"github.com/xraph/fabriq/core/livequery"
)

// Backend opens live-query subscriptions the gateway terminates. It is the only
// seam between the transport shell and the live-query tier;
// cluster.Gateway satisfies it through a small adapter (see forgeext), and tests
// substitute an in-memory fake.
type Backend interface {
	// Subscribe registers a live query and returns a handle whose Deltas
	// channel carries the snapshot (as OpReset + OpEnter rows) followed by live
	// deltas. The returned Sub MUST be Closed when the client disconnects.
	Subscribe(ctx context.Context, q livequery.LiveQuery) (*Sub, error)
}

// Sub is one terminated live subscription: a delta stream plus its control
// verbs. It mirrors livequery.Handle — captured state exposed as methods rather
// than a multi-value tuple — so callers manipulate the subscription, not a bag
// of loose values.
type Sub struct {
	// ID is the backend's subscription id (logging / client correlation).
	ID string
	// Deltas is the merged snapshot+live stream. It is closed by the backend
	// when the subscription ends.
	Deltas <-chan livequery.LiveDelta

	reanchor func(context.Context, *livequery.Cursor, int) error
	close    func()
}

// NewSub builds a Sub from its stream and control closures. The adapter in
// forgeext uses it to wrap a cluster.Gateway subscription; tests use it to wrap
// a hand-fed channel.
func NewSub(id string, deltas <-chan livequery.LiveDelta, reanchor func(context.Context, *livequery.Cursor, int) error, closeFn func()) *Sub {
	return &Sub{ID: id, Deltas: deltas, reanchor: reanchor, close: closeFn}
}

// Reanchor slides the maintained window to a new keyset anchor (deep/infinite
// scroll) in place, at O(window) server cost. No-op-safe if the backend did not
// supply a reanchor function.
func (s *Sub) Reanchor(ctx context.Context, cursor *livequery.Cursor, limit int) error {
	if s.reanchor == nil {
		return nil
	}
	return s.reanchor(ctx, cursor, limit)
}

// Close tears the subscription down (unsubscribe control to the owning shard).
// Idempotent-safe if the backend supplied no close function.
func (s *Sub) Close() {
	if s.close != nil {
		s.close()
	}
}
