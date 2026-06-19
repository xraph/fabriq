// Package fabriqtest is fabriq's exported test kit.
package fabriqtest

import (
	"context"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/query"
)

// fakeFabric adapts a World to the query.Fabric facade so consuming packages
// (core/agent, forgeext/agentmcp, forgeext) can test against the in-memory
// fakes without standing up the real Open() stack.
type fakeFabric struct {
	w *World
	x *command.Executor
}

// NewFabric returns a query.Fabric backed by the World's in-memory fakes. Exec
// runs through a command.Executor on the World's store (so writes are visible
// through Relational). Subscribe returns a fresh buffered channel (no deltas
// are published unless a test drives the underlying fakes); WaitForProjection
// is a no-op.
func NewFabric(w *World) query.Fabric {
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		panic(err) // registry+store are non-nil for a World
	}
	return &fakeFabric{w: w, x: x}
}

func (f *fakeFabric) Exec(ctx context.Context, cmd command.Command) (command.Result, error) {
	return f.x.Exec(ctx, cmd)
}

func (f *fakeFabric) ExecBatch(ctx context.Context, cmds []command.Command) ([]command.Result, error) {
	return f.x.ExecBatch(ctx, cmds)
}

func (f *fakeFabric) Relational() query.RelationalQuerier { return f.w.Rel }
func (f *fakeFabric) Graph() query.GraphQuerier           { return f.w.Graph }
func (f *fakeFabric) Search() query.SearchQuerier         { return f.w.Search }
func (f *fakeFabric) Timeseries() query.TSQuerier         { return f.w.TS }
func (f *fakeFabric) Vector() query.VectorQuerier         { return f.w.Vector }
func (f *fakeFabric) Spatial() query.SpatialQuerier       { return f.w.Spatial }
func (f *fakeFabric) Document() document.Store            { return f.w.Docs }
func (f *fakeFabric) Blob() blob.Store                    { return f.w.Blob }

func (f *fakeFabric) Subscribe(_ context.Context, _ query.SubscribeScope) (<-chan query.Delta, error) {
	return make(chan query.Delta), nil
}

func (f *fakeFabric) WaitForProjection(_ context.Context, _, _, _ string, _ int64) error {
	return nil
}

var _ query.Fabric = (*fakeFabric)(nil)
