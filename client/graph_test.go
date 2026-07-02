package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GraphNeighbors(t *testing.T) {
	var gotMethod, gotPath, gotType, gotID, gotLimit string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")
		gotID = r.URL.Query().Get("id")
		gotLimit = r.URL.Query().Get("limit")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(GraphData{
			Nodes: []GraphNode{
				{ID: "1", Type: "product", Label: "product", Props: map[string]any{"name": "gizmo"}},
			},
			Edges: []GraphEdge{
				{From: "1", To: "2", Rel: "RELATED_TO"},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	data, err := c.GraphNeighbors(context.Background(), GraphNeighborsParams{
		Type:  "product",
		ID:    "1",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("GraphNeighbors() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/graph/neighbors" {
		t.Errorf("path = %q, want /admin/graph/neighbors", gotPath)
	}
	if gotType != "product" {
		t.Errorf("query param type = %q, want %q", gotType, "product")
	}
	if gotID != "1" {
		t.Errorf("query param id = %q, want %q", gotID, "1")
	}
	if gotLimit != "10" {
		t.Errorf("query param limit = %q, want %q", gotLimit, "10")
	}

	if len(data.Nodes) != 1 {
		t.Fatalf("len(data.Nodes) = %d, want 1", len(data.Nodes))
	}
	if data.Nodes[0].ID != "1" || data.Nodes[0].Type != "product" {
		t.Errorf("data.Nodes[0] = %+v, want id=1 type=product", data.Nodes[0])
	}
	if data.Nodes[0].Props["name"] != "gizmo" {
		t.Errorf("data.Nodes[0].Props[name] = %v, want gizmo", data.Nodes[0].Props["name"])
	}
	if len(data.Edges) != 1 {
		t.Fatalf("len(data.Edges) = %d, want 1", len(data.Edges))
	}
	if data.Edges[0].From != "1" || data.Edges[0].To != "2" || data.Edges[0].Rel != "RELATED_TO" {
		t.Errorf("data.Edges[0] = %+v, want from=1 to=2 rel=RELATED_TO", data.Edges[0])
	}
}

func TestClient_GraphNeighbors_OmitsOptionalQueryParams(t *testing.T) {
	var gotRawQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(GraphData{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.GraphNeighbors(context.Background(), GraphNeighborsParams{Type: "product", ID: "1"}); err != nil {
		t.Fatalf("GraphNeighbors() unexpected error: %v", err)
	}

	if gotRawQuery != "id=1&type=product" {
		t.Errorf("raw query = %q, want %q", gotRawQuery, "id=1&type=product")
	}
}

func TestClient_GraphNeighbors_NotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "graph not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GraphNeighbors(context.Background(), GraphNeighborsParams{Type: "product", ID: "1"})
	if err == nil {
		t.Fatal("GraphNeighbors() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GraphNeighbors() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_GraphTraverse(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody GraphTraverseInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(GraphData{
			Nodes: []GraphNode{{ID: "1", Type: "product"}, {ID: "2", Type: "product"}},
			Edges: []GraphEdge{{From: "1", To: "2", Rel: "RELATED_TO"}},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	data, err := c.GraphTraverse(context.Background(), GraphTraverseInput{
		Type:  "product",
		ID:    "1",
		Depth: 2,
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("GraphTraverse() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/graph/traverse" {
		t.Errorf("path = %q, want /admin/graph/traverse", gotPath)
	}
	if gotBody.Type != "product" {
		t.Errorf("request body type = %q, want %q", gotBody.Type, "product")
	}
	if gotBody.ID != "1" {
		t.Errorf("request body id = %q, want %q", gotBody.ID, "1")
	}
	if gotBody.Depth != 2 {
		t.Errorf("request body depth = %d, want %d", gotBody.Depth, 2)
	}
	if gotBody.Limit != 100 {
		t.Errorf("request body limit = %d, want %d", gotBody.Limit, 100)
	}

	if len(data.Nodes) != 2 {
		t.Fatalf("len(data.Nodes) = %d, want 2", len(data.Nodes))
	}
	if len(data.Edges) != 1 {
		t.Fatalf("len(data.Edges) = %d, want 1", len(data.Edges))
	}
}

func TestClient_GraphQuery(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody GraphQueryInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(GraphQueryResult{
			Columns: []string{"id", "name"},
			Rows:    [][]any{{"1", "gizmo"}},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.GraphQuery(context.Background(), GraphQueryInput{
		Cypher: "MATCH (n) RETURN n.id AS id, n.name AS name LIMIT 5",
		Params: map[string]any{"x": 1},
	})
	if err != nil {
		t.Fatalf("GraphQuery() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/graph/query" {
		t.Errorf("path = %q, want /admin/graph/query", gotPath)
	}
	if gotBody.Cypher != "MATCH (n) RETURN n.id AS id, n.name AS name LIMIT 5" {
		t.Errorf("request body cypher = %q", gotBody.Cypher)
	}
	if gotBody.Params["x"] != float64(1) {
		t.Errorf("request body params[x] = %v, want 1", gotBody.Params["x"])
	}

	if len(result.Columns) != 2 || result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("result.Columns = %v, want [id name]", result.Columns)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 2 {
		t.Fatalf("result.Rows = %v, want one row of 2 cells", result.Rows)
	}
	if result.Rows[0][0] != "1" || result.Rows[0][1] != "gizmo" {
		t.Errorf("result.Rows[0] = %v, want [1 gizmo]", result.Rows[0])
	}
}

func TestClient_GraphQuery_MutatingRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"code":    "invalid_input",
				"message": "read-only queries only: CREATE/MERGE/DELETE/SET/REMOVE/DROP are not permitted",
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GraphQuery(context.Background(), GraphQueryInput{Cypher: "CREATE (n) RETURN n"})
	if err == nil {
		t.Fatal("GraphQuery() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GraphQuery() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}
