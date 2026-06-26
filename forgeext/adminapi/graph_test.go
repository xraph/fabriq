package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// postGraph issues a POST to a graph route with a JSON body and the test tenant
// header stamped.
func postGraph(t *testing.T, srv *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// notConfiguredGraph is a GraphQuerier whose every read answers with the
// store-not-configured sentinel, exactly as the package-private notConfigured
// stub an unconfigured fabriq.Open wires does. Used to exercise the 501 path.
type notConfiguredGraph struct{}

func (notConfiguredGraph) Query(context.Context, string, map[string]any, any) error {
	return fmt.Errorf("graph %w", fabriqerr.ErrStoreNotConfigured)
}

func (notConfiguredGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return fmt.Errorf("graph %w", fabriqerr.ErrStoreNotConfigured)
}

func (notConfiguredGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return fmt.Errorf("graph %w", fabriqerr.ErrStoreNotConfigured)
}

var _ query.GraphQuerier = notConfiguredGraph{}

// noGraphFabric wraps a real fake Fabric but swaps Graph() for an unconfigured
// stub, so the graph endpoints see a not-configured backend while every other
// port stays wired.
type noGraphFabric struct{ query.Fabric }

func (f noGraphFabric) Graph() query.GraphQuerier { return notConfiguredGraph{} }

// noGraphAdminExt builds an Extension whose fabric reports graph as
// not-configured. It reuses the tenant middleware so routes require X-Tenant-ID.
func noGraphAdminExt(t *testing.T, world *fabriqtest.World) *Extension {
	t.Helper()
	e := fakeBackedAdminExt(t, world)
	e.fabric = noGraphFabric{Fabric: fabriqtest.NewFabric(world)}
	return e
}

// TestGraphNeighbors_MissingType verifies that omitting ?type= returns 400.
func TestGraphNeighbors_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/graph/neighbors?id=x")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGraphNeighbors_MissingID verifies that omitting ?id= returns 400.
func TestGraphNeighbors_MissingID(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/graph/neighbors?type=widget")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGraphNeighbors_NotConfigured verifies a 501 when the graph backend is not
// configured.
func TestGraphNeighbors_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := noGraphAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/graph/neighbors?type=widget&id=x")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["error"] == "" {
		t.Error("expected non-empty error message")
	}
}

// TestGraphTraverse_MissingType verifies a body with no type returns 400.
func TestGraphTraverse_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/traverse", map[string]any{"id": "x", "depth": 2})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGraphTraverse_MissingID verifies a body with no id returns 400.
func TestGraphTraverse_MissingID(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/traverse", map[string]any{"type": "widget", "depth": 2})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGraphTraverse_NotConfigured verifies a 501 when the graph backend is not
// configured.
func TestGraphTraverse_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := noGraphAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/traverse", map[string]any{"type": "widget", "id": "x", "depth": 2})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
}

// TestGraphQuery_Empty verifies an empty cypher returns 400.
func TestGraphQuery_Empty(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/query", map[string]any{"cypher": "   "})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGraphQuery_RejectsMutating verifies that mutating clause keywords are
// rejected with 400 BEFORE the query is issued (read-only playground). It checks
// each keyword independently.
func TestGraphQuery_RejectsMutating(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	mutating := []string{
		"CREATE (n:Foo {id:'1'}) RETURN n",
		"MATCH (n) SET n.x = 1 RETURN n",
		"MATCH (n) DELETE n",
		"MERGE (n:Foo {id:'1'})",
		"MATCH (n) REMOVE n.x",
		"match (n) create (m) return m", // lower-case must also be caught
	}
	for _, cy := range mutating {
		resp := postGraph(t, srv, "/admin/graph/query", map[string]any{"cypher": cy})
		if resp.StatusCode != http.StatusBadRequest {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("cypher %q: status = %d, want 400, body = %s", cy, resp.StatusCode, body)
		}
		resp.Body.Close()
	}
}

// TestGraphQuery_NotConfigured verifies a read-only query against an
// unconfigured graph backend returns 501.
func TestGraphQuery_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := noGraphAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postGraph(t, srv, "/admin/graph/query", map[string]any{"cypher": "MATCH (n) RETURN n.id LIMIT 5"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
}

// TestMeta_GraphCapability verifies the graph.read capability slug is advertised.
func TestMeta_GraphCapability(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, c := range got.Capabilities {
		if c == "graph.read" {
			found = true
		}
	}
	if !found {
		t.Error("capability \"graph.read\" not advertised")
	}
}

// TestContainsMutatingClause unit-tests the read-only guard directly: it must
// catch each mutating keyword (any case) and pass read-only statements.
func TestContainsMutatingClause(t *testing.T) {
	mutating := []string{"CREATE", "merge x", "MATCH () DELETE r", "set", "remove", "DROP INDEX"}
	for _, cy := range mutating {
		if !containsMutatingClause(cy) {
			t.Errorf("containsMutatingClause(%q) = false, want true", cy)
		}
	}
	readonly := []string{"MATCH (n) RETURN n", "MATCH (n)-[r]-(m) RETURN m.id LIMIT 5", "RETURN 1"}
	for _, cy := range readonly {
		if containsMutatingClause(cy) {
			t.Errorf("containsMutatingClause(%q) = true, want false", cy)
		}
	}
}
