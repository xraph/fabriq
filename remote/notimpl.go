package remote

import "errors"

// ErrNotImplemented is returned by the methods the remote surface does not wire:
// raw-SQL Query, WaitForProjection, the projection-internal ApplyMutations /
// Graph.TraverseAndHydrate, and the optional blob (multipart/range) and document
// (history) sub-interfaces. The relational/projection reads, Timeseries, Spatial,
// blob List/Copy, the Document plane and live-query Reanchor are all wired now.
// It is deliberately distinct from ErrStoreNotConfigured: the store may well be
// configured server-side — the remote transport for that method just isn't built
// (ADR 0009).
var ErrNotImplemented = errors.New("remote: not implemented over the remote transport (see ADR 0009)")
