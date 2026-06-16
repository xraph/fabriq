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

// MemberLister returns every aggregate id currently matching a query's filter
// (no ordering, no payloads). It seeds a Streamed subscription's membership set
// so +match/-unmatch transitions are exact even for sets too large to
// materialize. Optional: when absent, Streamed mode seeds from the snapshot
// page instead (exact only up to that page).
type MemberLister interface {
	Members(ctx context.Context, q LiveQuery) ([]string, error)
}
