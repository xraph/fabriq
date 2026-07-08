package remote

import (
	"context"
	"errors"

	"github.com/xraph/fabriq/core/query"
)

// ErrNotImplemented is returned by the methods the remote surface does not wire
// (raw-SQL Query, the spatial port, and projection-plane-internal methods such
// as ApplyMutations). It is deliberately distinct from ErrStoreNotConfigured:
// the store may well be configured server-side — the remote transport for that
// method just isn't built (ADR 0009).
var ErrNotImplemented = errors.New("remote: not implemented over the remote transport (see ADR 0009)")

// niSpatial is a placeholder querier for the spatial port, which the remote
// surface does not yet wire. The relational, graph, search, vector and
// timeseries ports ARE wired (see relational.go, projections.go, timeseries.go).

type niSpatial struct{}

func (niSpatial) Upsert(context.Context, string, string, query.Geometry, map[string]any) error {
	return ErrNotImplemented
}
func (niSpatial) Within(context.Context, query.SpatialQuery, any) error { return ErrNotImplemented }
func (niSpatial) Get(context.Context, string, string) (geom query.Geometry, meta map[string]any, ok bool, err error) {
	return query.Geometry{}, nil, false, ErrNotImplemented
}
func (niSpatial) Delete(context.Context, string, string) error { return ErrNotImplemented }

var _ query.SpatialQuerier = niSpatial{}
