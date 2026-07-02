package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_RunQuery(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody QueryInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(QueryResult{
			Columns:   []string{"id", "name"},
			Rows:      []map[string]any{{"id": "1", "name": "widget"}},
			RowCount:  1,
			Truncated: false,
			ElapsedMs: 5,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.RunQuery(context.Background(), QueryInput{
		SQL:  "SELECT id, name FROM product WHERE id = $1",
		Args: []any{"1"},
	})
	if err != nil {
		t.Fatalf("RunQuery() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/query" {
		t.Errorf("path = %q, want /admin/query", gotPath)
	}
	if gotBody.SQL != "SELECT id, name FROM product WHERE id = $1" {
		t.Errorf("request body sql = %q, want the query text", gotBody.SQL)
	}
	if len(gotBody.Args) != 1 || gotBody.Args[0] != "1" {
		t.Errorf("request body args = %+v, want [1]", gotBody.Args)
	}
	if result.RowCount != 1 || len(result.Rows) != 1 {
		t.Fatalf("result = %+v, want rowCount=1 with 1 row", result)
	}
	if result.Rows[0]["name"] != "widget" {
		t.Errorf("result.Rows[0][name] = %v, want widget", result.Rows[0]["name"])
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("result.Columns = %v, want [id name]", result.Columns)
	}
}

func TestClient_RunQuery_BadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "only read-only SELECT/WITH queries are allowed"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RunQuery(context.Background(), QueryInput{SQL: "DELETE FROM product"})
	if err == nil {
		t.Fatal("RunQuery() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("RunQuery() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}

func TestClient_RunQuery_NotImplemented(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "raw query not available (no relational store opened)"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RunQuery(context.Background(), QueryInput{SQL: "SELECT 1"})
	if err == nil {
		t.Fatal("RunQuery() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("RunQuery() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}
