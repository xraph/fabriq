package livequery

import "context"

// Snapshotter returns the first `limit` rows from a live query's anchor in
// total order (Sort…, id). Implemented by adapters/postgres (RLS-enforced).
type Snapshotter interface {
	Snapshot(ctx context.Context, q LiveQuery, limit int) ([]Row, error)
}

// Refiller returns up to `limit` rows strictly after `after` in total order —
// the bounded keyset boundary refill that keeps the window an exact prefix.
type Refiller interface {
	After(ctx context.Context, q LiveQuery, after Cursor, limit int) ([]Row, error)
}
