package remote

import (
	"context"
	"errors"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
)

// ErrNotImplemented is returned by the planes RemoteFabric does not yet wire
// (read, live, blob, interactive tx). It is deliberately distinct from
// ErrStoreNotConfigured: the store may well be configured server-side — the
// remote transport for that plane just isn't built yet (ADR 0009).
var ErrNotImplemented = errors.New("remote: plane not implemented (read/live/blob not yet wired; see ADR 0009)")

// The ni* types are placeholder queriers for the projection-read ports not yet
// wired, so RemoteFabric satisfies query.Fabric as a drop-in today. Each method
// returns ErrNotImplemented. They are separate types (not one) because
// VectorQuerier.Upsert and SpatialQuerier.Upsert collide in signature. (The
// relational port is wired — see relational.go.)

type niGraph struct{}

func (niGraph) Query(context.Context, string, map[string]any, any) error { return ErrNotImplemented }
func (niGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return ErrNotImplemented
}
func (niGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return ErrNotImplemented
}

type niSearch struct{}

func (niSearch) Search(context.Context, query.SearchQuery, any) error { return ErrNotImplemented }
func (niSearch) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return ErrNotImplemented
}

type niTS struct{}

func (niTS) BulkWrite(context.Context, string, []query.Point) error { return ErrNotImplemented }
func (niTS) Range(context.Context, query.RangeQuery, any) error     { return ErrNotImplemented }

type niVector struct{}

func (niVector) Upsert(context.Context, string, string, []float32, map[string]any) error {
	return ErrNotImplemented
}
func (niVector) Similar(context.Context, query.VectorQuery, any) error { return ErrNotImplemented }
func (niVector) Delete(context.Context, string, string) error          { return ErrNotImplemented }

type niSpatial struct{}

func (niSpatial) Upsert(context.Context, string, string, query.Geometry, map[string]any) error {
	return ErrNotImplemented
}
func (niSpatial) Within(context.Context, query.SpatialQuery, any) error { return ErrNotImplemented }
func (niSpatial) Delete(context.Context, string, string) error          { return ErrNotImplemented }

var (
	_ query.GraphQuerier   = niGraph{}
	_ query.SearchQuerier  = niSearch{}
	_ query.TSQuerier      = niTS{}
	_ query.VectorQuerier  = niVector{}
	_ query.SpatialQuerier = niSpatial{}
)
