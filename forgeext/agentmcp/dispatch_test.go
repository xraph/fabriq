package agentmcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// --- minimal fake Fabric over a fabriqtest.World (local to this package) ---
type fakeFabric struct {
	w     *fabriqtest.World
	x     *command.Executor
	subCh chan query.Delta
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

// Analytics implements query.Fabric with a package-local not-configured stub;
// no dispatch test exercises analytics.
func (f *fakeFabric) Analytics() query.AnalyticsQuerier { return notConfiguredAnalytics{} }

func (f *fakeFabric) Subscribe(context.Context, query.SubscribeScope) (<-chan query.Delta, error) {
	if f.subCh == nil {
		f.subCh = make(chan query.Delta, 16)
	}
	return f.subCh, nil
}

func (f *fakeFabric) pushDelta(d query.Delta) {
	if f.subCh == nil {
		f.subCh = make(chan query.Delta, 16)
	}
	f.subCh <- d
}
func (f *fakeFabric) WaitForProjection(context.Context, string, string, string, int64) error {
	return nil
}

var _ query.Fabric = (*fakeFabric)(nil)

// notConfiguredAnalytics is a package-local stand-in for query.AnalyticsQuerier
// (this test package predates the analytics port).
type notConfiguredAnalytics struct{}

func (notConfiguredAnalytics) Track(context.Context, []query.AnalyticsEvent) error {
	return fabriqerr.ErrStoreNotConfigured
}

func (notConfiguredAnalytics) Query(context.Context, query.AnalyticsQuery, any) error {
	return fabriqerr.ErrStoreNotConfigured
}

func (notConfiguredAnalytics) QueryRaw(context.Context, any, string, ...any) error {
	return fabriqerr.ErrStoreNotConfigured
}

type mcpDoc struct {
	grove.BaseModel `grove:"table:mcpdocs"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Title           string `grove:"title,notnull"`
}

func newToolkit(t testing.TB) (*agent.Toolkit, *fakeFabric, context.Context) {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{Name: "mcpdoc", Kind: registry.KindAggregate, Model: (*mcpDoc)(nil)})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	w := fabriqtest.NewWorld(r)
	ff := newFakeFabric(t, w)
	tk, err := agent.NewToolkit(ff, r, nil, agent.Config{Write: agent.WritePolicy{Allow: map[string][]command.Op{"mcpdoc": {command.OpCreate}}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return tk, ff, ctx
}

func TestDispatch_ToolsList(t *testing.T) {
	tk, _, ctx := newToolkit(t)
	resp := Dispatch(ctx, tk, []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))

	var out struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
		Error *struct{} `json:"error"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("bad response %s: %v", resp, err)
	}
	if out.Error != nil {
		t.Fatalf("unexpected error: %s", resp)
	}
	names := map[string]bool{}
	for _, tl := range out.Result.Tools {
		names[tl.Name] = true
	}
	for _, want := range []string{"recall", "search", "get", "remember", "graph_traverse", "vector_similar"} {
		if !names[want] {
			t.Fatalf("tools/list missing %q: %s", want, resp)
		}
	}
}

func TestDispatch_ToolsCallRemember(t *testing.T) {
	tk, ff, ctx := newToolkit(t)
	resp := Dispatch(ctx, tk, []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"remember","arguments":{"entity":"mcpdoc","op":"create","payload":{"title":"hi"}}}}`))

	var out struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("bad response %s: %v", resp, err)
	}
	if out.Result.IsError {
		t.Fatalf("remember returned isError: %s", resp)
	}
	_ = ff
}

func TestDispatch_ToolErrorIsErrorResult(t *testing.T) {
	tk, _, ctx := newToolkit(t)
	// remember on a non-allowed entity → tool returns a WriteError → isError result
	resp := Dispatch(ctx, tk, []byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"remember","arguments":{"entity":"ghost","op":"create","payload":{}}}}`))
	var out struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("bad response %s: %v", resp, err)
	}
	if !out.Result.IsError {
		t.Fatalf("want isError for denied write, got %s", resp)
	}
}

func TestDispatch_UnknownToolAndMethod(t *testing.T) {
	tk, _, ctx := newToolkit(t)
	r1 := Dispatch(ctx, tk, []byte(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"nope","arguments":{}}}`))
	if !hasJSONRPCError(r1) {
		t.Fatalf("unknown tool should be JSON-RPC error: %s", r1)
	}
	r2 := Dispatch(ctx, tk, []byte(`{"jsonrpc":"2.0","id":5,"method":"frobnicate"}`))
	if !hasJSONRPCError(r2) {
		t.Fatalf("unknown method should be JSON-RPC error: %s", r2)
	}
	r3 := Dispatch(ctx, tk, []byte(`{not json`))
	if !hasJSONRPCError(r3) {
		t.Fatalf("parse error should be JSON-RPC error: %s", r3)
	}
}

func hasJSONRPCError(resp []byte) bool {
	var out struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(resp, &out)
	return out.Error != nil
}
