package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
)

func TestClient_SearchText(t *testing.T) {
	var gotMethod, gotPath, gotType, gotQuery, gotLimit, gotOffset, gotSort string
	var gotFilters []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")
		gotQuery = r.URL.Query().Get("q")
		gotLimit = r.URL.Query().Get("limit")
		gotOffset = r.URL.Query().Get("offset")
		gotSort = r.URL.Query().Get("sort")
		gotFilters = r.URL.Query()["filter"]
		sort.Strings(gotFilters)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SearchPage{
			Items: []EntityRecord{
				{ID: "1", Type: "widget", Data: map[string]any{"name": "gizmo"}},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	page, err := c.SearchText(context.Background(), SearchTextParams{
		Type:   "widget",
		Query:  "gizmo",
		Limit:  10,
		Offset: 5,
		Sort:   "name DESC",
		Filter: map[string]string{"status": "active", "color": "red"},
	})
	if err != nil {
		t.Fatalf("SearchText() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/search" {
		t.Errorf("path = %q, want /admin/search", gotPath)
	}
	if gotType != "widget" {
		t.Errorf("query param type = %q, want %q", gotType, "widget")
	}
	if gotQuery != "gizmo" {
		t.Errorf("query param q = %q, want %q", gotQuery, "gizmo")
	}
	if gotLimit != "10" {
		t.Errorf("query param limit = %q, want %q", gotLimit, "10")
	}
	if gotOffset != "5" {
		t.Errorf("query param offset = %q, want %q", gotOffset, "5")
	}
	if gotSort != "name DESC" {
		t.Errorf("query param sort = %q, want %q", gotSort, "name DESC")
	}
	wantFilters := []string{"color:red", "status:active"}
	if len(gotFilters) != len(wantFilters) || gotFilters[0] != wantFilters[0] || gotFilters[1] != wantFilters[1] {
		t.Errorf("query params filter = %v, want %v", gotFilters, wantFilters)
	}

	if len(page.Items) != 1 {
		t.Fatalf("len(page.Items) = %d, want 1", len(page.Items))
	}
	if page.Items[0].ID != "1" || page.Items[0].Type != "widget" {
		t.Errorf("page.Items[0] = %+v, want id=1 type=widget", page.Items[0])
	}
	if page.Items[0].Data["name"] != "gizmo" {
		t.Errorf("page.Items[0].Data[name] = %v, want gizmo", page.Items[0].Data["name"])
	}
}

func TestClient_SearchText_OmitsOptionalQueryParams(t *testing.T) {
	var gotRawQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SearchPage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.SearchText(context.Background(), SearchTextParams{Type: "widget", Query: "gizmo"}); err != nil {
		t.Fatalf("SearchText() unexpected error: %v", err)
	}

	if gotRawQuery != "q=gizmo&type=widget" {
		t.Errorf("raw query = %q, want %q", gotRawQuery, "q=gizmo&type=widget")
	}
}
