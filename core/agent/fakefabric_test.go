// core/agent/fakefabric_test.go
package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

var errNotImplemented = errors.New("agent test: not implemented in phase 1a")

// fakeFabric adapts a fabriqtest.World to the query.Fabric interface. Phase-1a
// recall exercises only the read queriers; Exec is wired so tests can seed
// rows; subscribe/wait are stubbed (unused in 1a). Phase-3 adds Subscribe
// test-driving via subCh and lastSubscribeScope.
type fakeFabric struct {
	w                  *fabriqtest.World
	x                  *command.Executor
	subCh              chan query.Delta
	lastSubscribeScope query.SubscribeScope
}

func newFakeFabric(t testing.TB, w *fabriqtest.World) *fakeFabric {
	t.Helper()
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		t.Fatal(err)
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
func (f *fakeFabric) Subscribe(_ context.Context, scope query.SubscribeScope) (<-chan query.Delta, error) {
	f.lastSubscribeScope = scope
	if f.subCh == nil {
		f.subCh = make(chan query.Delta, 16)
	}
	return f.subCh, nil
}

// pushDelta feeds a delta to the most recent Subscribe channel (test helper).
func (f *fakeFabric) pushDelta(d query.Delta) {
	if f.subCh == nil {
		f.subCh = make(chan query.Delta, 16)
	}
	f.subCh <- d
}
func (f *fakeFabric) WaitForProjection(context.Context, string, string, string, int64) error {
	return errNotImplemented
}

var _ query.Fabric = (*fakeFabric)(nil)

// tDoc is the shared test aggregate.
type tDoc struct {
	grove.BaseModel `grove:"table:docs"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Title    string `grove:"title,notnull"`
	Body     string `grove:"body"`
}

func testRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "doc", Kind: registry.KindAggregate, Model: (*tDoc)(nil), GraphNode: "Doc",
		Search:    registry.SearchSpec{Index: "docs", Fields: []string{"title", "body"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func testCtx(t testing.TB, tid string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tid)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
