package remote

import (
	"context"
	"errors"

	"github.com/xraph/fabriq/core/query"
)

// ErrNotImplemented is returned by the methods the remote surface does not wire
// (raw-SQL Query, the timeseries/spatial ports, and projection-plane-internal
// methods such as ApplyMutations). It is deliberately distinct from
// ErrStoreNotConfigured: the store may well be configured server-side — the
// remote transport for that method just isn't built (ADR 0009).
var ErrNotImplemented = errors.New("remote: not implemented over the remote transport (see ADR 0009)")

// niTS and niSpatial are placeholder queriers for the timeseries and spatial
// ports, which the remote surface does not yet wire. The relational, graph,
// search and vector ports ARE wired (see relational.go, projections.go).

type niTS struct{}

func (niTS) BulkWrite(context.Context, string, []query.Point) error { return ErrNotImplemented }
func (niTS) Range(context.Context, query.RangeQuery, any) error     { return ErrNotImplemented }

type niSpatial struct{}

func (niSpatial) Upsert(context.Context, string, string, query.Geometry, map[string]any) error {
	return ErrNotImplemented
}
func (niSpatial) Within(context.Context, query.SpatialQuery, any) error { return ErrNotImplemented }
func (niSpatial) Delete(context.Context, string, string) error          { return ErrNotImplemented }

var (
	_ query.TSQuerier      = niTS{}
	_ query.SpatialQuerier = niSpatial{}
)
