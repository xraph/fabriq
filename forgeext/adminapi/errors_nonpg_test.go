package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// readErrorBody reads the full response body, decodes it as the structured
// errorBody shape, and returns both the decoded body and the raw bytes so
// callers can additionally scan the raw text for a leaked backend string.
// Decoding into the strict errorBody shape (rather than a generic map) is
// itself part of the leak probe: the old forge.InternalError envelope carries
// a different top-level shape ({"error":"...","details":"<raw err>"} — a
// bare string, not an {code,message,...} object), so a body that still used
// the legacy envelope would fail to decode Error.Code as expected/would leave
// it empty.
func readErrorBody(t *testing.T, resp *http.Response) (errorBody, string) {
	t.Helper()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var body errorBody
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("body not valid errorBody JSON (leaked raw text?): %v; raw = %s", err, raw)
	}
	return body, string(raw)
}

// faultyGraph is a query.GraphQuerier whose Query answers with a
// driver-shaped, unstructured error (the same shape a raw FalkorDB client
// error takes before the adapter's translator wraps it). It exercises the
// leak probe end-to-end: a real backend failure flowing through the admin
// HTTP layer must never surface the raw driver string in the response body.
type faultyGraph struct{ err error }

func (f faultyGraph) Query(context.Context, string, map[string]any, any) error {
	return f.err
}

func (f faultyGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return f.err
}

func (f faultyGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return f.err
}

var _ query.GraphQuerier = faultyGraph{}

// faultyGraphFabric wraps a real fake Fabric but swaps Graph() for a faulty
// stub, so the graph endpoints see a backend failure while every other port
// stays wired (mirrors noGraphFabric in graph_test.go, but injects a fault
// instead of not-configured).
type faultyGraphFabric struct {
	query.Fabric
	err error
}

func (f faultyGraphFabric) Graph() query.GraphQuerier { return faultyGraph{err: f.err} }

// faultyGraphAdminExt builds an Extension whose fabric's graph backend fails
// every Query call with err.
func faultyGraphAdminExt(t *testing.T, world *fabriqtest.World, err error) *Extension {
	t.Helper()
	e := fakeBackedAdminExt(t, world)
	e.fabric = faultyGraphFabric{Fabric: fabriqtest.NewFabric(world), err: err}
	return e
}

// TestGraphQuery_UnstructuredBackendErrorDoesNotLeak drives POST
// /admin/graph/query (graph.go:337's g.Query call) against a graph backend
// that fails with a raw, driver-shaped error string (unstructured — no
// *fabriqerr.Error, mirroring an untranslated client error). It asserts the
// response is the structured errorBody (error.code == "internal"), NOT the
// legacy forge.InternalError "details" envelope, and that the injected driver
// string/secret never appears anywhere in the raw response body.
func TestGraphQuery_UnstructuredBackendErrorDoesNotLeak(t *testing.T) {
	world := buildTestWorld(t)
	injected := fmt.Errorf("falkordb: dial tcp 10.0.0.9:6379: connection refused (secret-token=abc123)")
	e := faultyGraphAdminExt(t, world, injected)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/query", map[string]any{"cypher": "MATCH (n) RETURN n.id LIMIT 5"})
	if resp.StatusCode != http.StatusInternalServerError {
		resp.Body.Close()
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	body, raw := readErrorBody(t, resp)

	if body.Error.Code != string(fabriqerr.CodeInternal) {
		t.Fatalf("code = %q, want %q", body.Error.Code, fabriqerr.CodeInternal)
	}
	if strings.Contains(raw, "falkordb:") || strings.Contains(raw, "secret-token") {
		t.Fatalf("raw backend error leaked into response body: %s", raw)
	}
	// Sanity: this is NOT the legacy forge envelope shape (no top-level
	// "details" string carrying the raw error).
	var legacy map[string]any
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		t.Fatalf("re-decode as map: %v", err)
	}
	if _, ok := legacy["details"]; ok {
		t.Fatalf("response still uses the legacy forge.InternalError envelope (has \"details\"): %s", raw)
	}
}

// TestGraphNeighbors_UnstructuredBackendErrorDoesNotLeak is the same probe for
// the 1-hop neighbours endpoint (graph.go:179/193's g.Query call sites).
func TestGraphNeighbors_UnstructuredBackendErrorDoesNotLeak(t *testing.T) {
	world := buildTestWorld(t)
	injected := fmt.Errorf("falkordb: read timeout after 5s on conn 0xdeadbeef")
	e := faultyGraphAdminExt(t, world, injected)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/graph/neighbors?type=widget&id=x")
	if resp.StatusCode != http.StatusInternalServerError {
		resp.Body.Close()
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	body, raw := readErrorBody(t, resp)

	if body.Error.Code != string(fabriqerr.CodeInternal) {
		t.Fatalf("code = %q, want %q", body.Error.Code, fabriqerr.CodeInternal)
	}
	if strings.Contains(raw, "falkordb:") || strings.Contains(raw, "deadbeef") {
		t.Fatalf("raw backend error leaked into response body: %s", raw)
	}
}

// faultyCache is a corecache.Cache whose every method fails with a
// driver-shaped, unstructured error, used to fault-inject
// handleCacheInvalidate (cache_admin.go:176's cache.InvalidateEntity call).
type faultyCache struct{ err error }

func (f faultyCache) GetOrLoad(_ context.Context, _ corecache.Keyspace, _ string,
	_ func(context.Context) ([]byte, error)) ([]byte, error) {
	return nil, f.err
}
func (f faultyCache) Get(context.Context, corecache.Keyspace, string) ([]byte, bool, error) {
	return nil, false, f.err
}
func (f faultyCache) Set(context.Context, corecache.Keyspace, string, []byte) error { return f.err }
func (f faultyCache) Invalidate(context.Context, corecache.Keyspace, ...string) error {
	return f.err
}
func (f faultyCache) InvalidateKeyspace(context.Context, corecache.Keyspace) error { return f.err }
func (f faultyCache) InvalidateEntity(context.Context, string) error               { return f.err }
func (f faultyCache) Close() error                                                 { return nil }

var _ corecache.Cache = faultyCache{}

// TestCacheInvalidate_UnstructuredBackendErrorDoesNotLeak drives POST
// /admin/cache/invalidate against a cache backend (e.g. Redis) whose
// InvalidateEntity call fails with a raw, driver-shaped error. The Extension's
// cache field is unexported but this test lives in the same package, so the
// fake is wired directly (no fabriqtest seam needed here) — the simplest
// fault-injection path available for this handler. The widget entity is
// patched with a CacheSpec in-place (the *registry.Entity Get returns is the
// live stored pointer) so handleCacheInvalidate's "entity is not cached"
// guard passes and the flow reaches cache.InvalidateEntity.
func TestCacheInvalidate_UnstructuredBackendErrorDoesNotLeak(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)

	ent, ok := e.reg.Get("widget")
	if !ok {
		t.Fatal("widget entity not registered")
	}
	ent.Spec.Cache = &registry.CacheSpec{TTL: time.Minute}

	injected := fmt.Errorf("redigo: dial tcp 10.0.0.5:6379: i/o timeout (auth=hunter2)")
	e.cache = faultyCache{err: injected}

	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/cache/invalidate",
		map[string]any{"entity": "widget"})
	if resp.StatusCode != http.StatusInternalServerError {
		resp.Body.Close()
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	body, raw := readErrorBody(t, resp)

	if body.Error.Code != string(fabriqerr.CodeInternal) {
		t.Fatalf("code = %q, want %q", body.Error.Code, fabriqerr.CodeInternal)
	}
	if strings.Contains(raw, "redigo:") || strings.Contains(raw, "hunter2") {
		t.Fatalf("raw backend error leaked into response body: %s", raw)
	}
}
