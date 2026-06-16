package fabriq

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
)

// Unconfigured capability ports fail loudly with the typed sentinel rather
// than panicking on a nil interface.

func errPort(name string) error {
	return fmt.Errorf("fabriq: %s port: %w", name, ErrStoreNotConfigured)
}

type notConfiguredGraph struct{}

func (notConfiguredGraph) Query(context.Context, string, map[string]any, any) error {
	return errPort("graph")
}
func (notConfiguredGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return errPort("graph")
}
func (notConfiguredGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return errPort("graph")
}

type notConfiguredSearch struct{}

func (notConfiguredSearch) Search(context.Context, query.SearchQuery, any) error {
	return errPort("search")
}
func (notConfiguredSearch) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return errPort("search")
}

type notConfiguredTS struct{}

func (notConfiguredTS) BulkWrite(context.Context, string, []query.Point) error {
	return errPort("timeseries")
}
func (notConfiguredTS) Range(context.Context, query.RangeQuery, any) error {
	return errPort("timeseries")
}

type notConfiguredVector struct{}

func (notConfiguredVector) Upsert(context.Context, string, string, []float32, map[string]any) error {
	return errPort("vector")
}
func (notConfiguredVector) Similar(context.Context, query.VectorQuery, any) error {
	return errPort("vector")
}

type notConfiguredSpatial struct{}

func (notConfiguredSpatial) Upsert(context.Context, string, string, query.Geometry, map[string]any) error {
	return errPort("spatial")
}
func (notConfiguredSpatial) Within(context.Context, query.SpatialQuery, any) error {
	return errPort("spatial")
}
func (notConfiguredSpatial) Delete(context.Context, string, string) error {
	return errPort("spatial")
}

type notConfiguredDocs struct{}

func (notConfiguredDocs) ApplyUpdate(context.Context, string, []byte) error {
	return errPort("document")
}
func (notConfiguredDocs) Sync(context.Context, string, []byte) ([]byte, error) {
	return nil, errPort("document")
}
func (notConfiguredDocs) Snapshot(context.Context, string) (document.Materialized, error) {
	return document.Materialized{}, errPort("document")
}
func (notConfiguredDocs) Compact(context.Context, string) error {
	return errPort("document")
}
