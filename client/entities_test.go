package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_ListEntities(t *testing.T) {
	var gotMethod, gotPath, gotType, gotLimit, gotCursor string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")
		gotLimit = r.URL.Query().Get("limit")
		gotCursor = r.URL.Query().Get("cursor")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntityPage{
			Items: []EntityRecord{
				{ID: "1", Type: "widget", Data: map[string]any{"name": "gizmo"}},
			},
			NextCursor: "50",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	page, err := c.ListEntities(context.Background(), ListEntitiesParams{
		Type:   "widget",
		Limit:  10,
		Cursor: "0",
	})
	if err != nil {
		t.Fatalf("ListEntities() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/entities" {
		t.Errorf("path = %q, want /admin/entities", gotPath)
	}
	if gotType != "widget" {
		t.Errorf("query param type = %q, want %q", gotType, "widget")
	}
	if gotLimit != "10" {
		t.Errorf("query param limit = %q, want %q", gotLimit, "10")
	}
	if gotCursor != "0" {
		t.Errorf("query param cursor = %q, want %q", gotCursor, "0")
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
	if page.NextCursor != "50" {
		t.Errorf("page.NextCursor = %q, want %q", page.NextCursor, "50")
	}
}

func TestClient_ListEntities_OmitsOptionalQueryParams(t *testing.T) {
	var gotRawQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntityPage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.ListEntities(context.Background(), ListEntitiesParams{Type: "widget"}); err != nil {
		t.Fatalf("ListEntities() unexpected error: %v", err)
	}

	if gotRawQuery != "type=widget" {
		t.Errorf("raw query = %q, want %q", gotRawQuery, "type=widget")
	}
}

func TestClient_GetEntity(t *testing.T) {
	var gotMethod, gotPath, gotType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntityRecord{
			ID:   "42",
			Type: "widget",
			Data: map[string]any{"name": "gizmo"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	rec, err := c.GetEntity(context.Background(), "42", "widget")
	if err != nil {
		t.Fatalf("GetEntity() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/entities/42" {
		t.Errorf("path = %q, want /admin/entities/42", gotPath)
	}
	if gotType != "widget" {
		t.Errorf("query param type = %q, want %q", gotType, "widget")
	}
	if rec.ID != "42" || rec.Type != "widget" {
		t.Errorf("rec = %+v, want id=42 type=widget", rec)
	}
	if rec.Data["name"] != "gizmo" {
		t.Errorf("rec.Data[name] = %v, want gizmo", rec.Data["name"])
	}
}

func TestClient_CreateEntity(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody EntityWriteInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(EntityRecord{
			ID:   "new-1",
			Type: gotBody.Type,
			Data: gotBody.Data,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	rec, err := c.CreateEntity(context.Background(), EntityWriteInput{
		Type: "widget",
		Data: map[string]any{"name": "gizmo"},
	})
	if err != nil {
		t.Fatalf("CreateEntity() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/entities" {
		t.Errorf("path = %q, want /admin/entities", gotPath)
	}
	if gotBody.Type != "widget" {
		t.Errorf("request body type = %q, want %q", gotBody.Type, "widget")
	}
	if gotBody.Data["name"] != "gizmo" {
		t.Errorf("request body data[name] = %v, want gizmo", gotBody.Data["name"])
	}
	if rec.ID != "new-1" {
		t.Errorf("rec.ID = %q, want %q", rec.ID, "new-1")
	}
}

func TestClient_UpdateEntity(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody EntityWriteInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntityRecord{
			ID:   "42",
			Type: gotBody.Type,
			Data: gotBody.Data,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	rec, err := c.UpdateEntity(context.Background(), "42", EntityWriteInput{
		Type: "widget",
		Data: map[string]any{"name": "updated"},
	})
	if err != nil {
		t.Fatalf("UpdateEntity() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotPath != "/admin/entities/42" {
		t.Errorf("path = %q, want /admin/entities/42", gotPath)
	}
	if gotBody.Type != "widget" {
		t.Errorf("request body type = %q, want %q", gotBody.Type, "widget")
	}
	if gotBody.Data["name"] != "updated" {
		t.Errorf("request body data[name] = %v, want updated", gotBody.Data["name"])
	}
	if rec.Data["name"] != "updated" {
		t.Errorf("rec.Data[name] = %v, want updated", rec.Data["name"])
	}
}

func TestClient_DeleteEntity(t *testing.T) {
	var gotMethod, gotPath, gotType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if err := c.DeleteEntity(context.Background(), "42", "widget"); err != nil {
		t.Fatalf("DeleteEntity() unexpected error: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/admin/entities/42" {
		t.Errorf("path = %q, want /admin/entities/42", gotPath)
	}
	if gotType != "widget" {
		t.Errorf("query param type = %q, want %q", gotType, "widget")
	}
}
