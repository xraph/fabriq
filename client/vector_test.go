package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_SearchVector(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody VectorSearchInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorSearchPage{
			Matches: []VectorMatch{
				{ID: "1", Score: 0.98, Data: map[string]any{"name": "gizmo"}},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	page, err := c.SearchVector(context.Background(), VectorSearchInput{
		Type:   "widget",
		Query:  "gizmo",
		K:      5,
		Filter: map[string]string{"status": "active"},
	})
	if err != nil {
		t.Fatalf("SearchVector() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/search/vector" {
		t.Errorf("path = %q, want /admin/search/vector", gotPath)
	}
	if gotBody.Type != "widget" {
		t.Errorf("request body type = %q, want %q", gotBody.Type, "widget")
	}
	if gotBody.Query != "gizmo" {
		t.Errorf("request body query = %q, want %q", gotBody.Query, "gizmo")
	}
	if gotBody.K != 5 {
		t.Errorf("request body k = %d, want %d", gotBody.K, 5)
	}
	if gotBody.Filter["status"] != "active" {
		t.Errorf("request body filter[status] = %q, want %q", gotBody.Filter["status"], "active")
	}

	if len(page.Matches) != 1 {
		t.Fatalf("len(page.Matches) = %d, want 1", len(page.Matches))
	}
	if page.Matches[0].ID != "1" || page.Matches[0].Score != 0.98 {
		t.Errorf("page.Matches[0] = %+v, want id=1 score=0.98", page.Matches[0])
	}
	if page.Matches[0].Data["name"] != "gizmo" {
		t.Errorf("page.Matches[0].Data[name] = %v, want gizmo", page.Matches[0].Data["name"])
	}
}

func TestClient_SearchVector_SimilarToEntity(t *testing.T) {
	var gotBody VectorSearchInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorSearchPage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.SearchVector(context.Background(), VectorSearchInput{Type: "widget", ID: "42"}); err != nil {
		t.Fatalf("SearchVector() unexpected error: %v", err)
	}

	if gotBody.ID != "42" {
		t.Errorf("request body id = %q, want %q", gotBody.ID, "42")
	}
	if gotBody.Query != "" {
		t.Errorf("request body query = %q, want empty", gotBody.Query)
	}
}

func TestClient_VectorGet(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorEmbeddingInfo{
			Entity:  "widget",
			ID:      "42",
			Dims:    768,
			Norm:    1.23,
			Preview: []float32{0.1, 0.2, 0.3},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	info, err := c.VectorGet(context.Background(), "widget", "42")
	if err != nil {
		t.Fatalf("VectorGet() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/vector/widget/42" {
		t.Errorf("path = %q, want /admin/vector/widget/42", gotPath)
	}
	if info.Entity != "widget" || info.ID != "42" {
		t.Errorf("info = %+v, want entity=widget id=42", info)
	}
	if info.Dims != 768 {
		t.Errorf("info.Dims = %d, want 768", info.Dims)
	}
	if len(info.Preview) != 3 {
		t.Errorf("len(info.Preview) = %d, want 3", len(info.Preview))
	}
}

func TestClient_VectorDelete(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorDeleteResult{Deleted: true})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.VectorDelete(context.Background(), "widget", "42")
	if err != nil {
		t.Fatalf("VectorDelete() unexpected error: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/admin/vector/widget/42" {
		t.Errorf("path = %q, want /admin/vector/widget/42", gotPath)
	}
	if !result.Deleted {
		t.Errorf("result.Deleted = %v, want true", result.Deleted)
	}
}

func TestClient_VectorDeleteByMeta(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody VectorDeleteByMetaInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorDeleteResult{Deleted: true})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.VectorDeleteByMeta(context.Background(), VectorDeleteByMetaInput{
		Entity: "widget",
		Filter: map[string]string{"status": "stale"},
	})
	if err != nil {
		t.Fatalf("VectorDeleteByMeta() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/vector/delete-by-meta" {
		t.Errorf("path = %q, want /admin/vector/delete-by-meta", gotPath)
	}
	if gotBody.Entity != "widget" {
		t.Errorf("request body entity = %q, want %q", gotBody.Entity, "widget")
	}
	if gotBody.Filter["status"] != "stale" {
		t.Errorf("request body filter[status] = %q, want %q", gotBody.Filter["status"], "stale")
	}
	if gotBody.All {
		t.Errorf("request body all = %v, want false", gotBody.All)
	}
	if !result.Deleted {
		t.Errorf("result.Deleted = %v, want true", result.Deleted)
	}
}

func TestClient_VectorDeleteByMeta_All(t *testing.T) {
	var gotBody VectorDeleteByMetaInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(VectorDeleteResult{Deleted: true})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.VectorDeleteByMeta(context.Background(), VectorDeleteByMetaInput{
		Entity: "widget",
		All:    true,
	}); err != nil {
		t.Fatalf("VectorDeleteByMeta() unexpected error: %v", err)
	}

	if !gotBody.All {
		t.Errorf("request body all = %v, want true", gotBody.All)
	}
	if len(gotBody.Filter) != 0 {
		t.Errorf("request body filter = %v, want empty", gotBody.Filter)
	}
}
