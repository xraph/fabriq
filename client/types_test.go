package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_ListEntityTypes(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"types": []string{"product", "site"}})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	types, err := c.ListEntityTypes(context.Background())
	if err != nil {
		t.Fatalf("ListEntityTypes() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/entities/types" {
		t.Errorf("path = %q, want /admin/entities/types", gotPath)
	}
	if len(types) != 2 || types[0] != "product" || types[1] != "site" {
		t.Errorf("types = %v, want [product site]", types)
	}
}

func TestClient_GetEntitySchema(t *testing.T) {
	var gotMethod, gotPath, gotType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntitySchema{
			Type: "product",
			Fields: []SchemaField{
				{Name: "id", Kind: "string", Required: true},
				{Name: "price", Kind: "number", Required: false},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	schema, err := c.GetEntitySchema(context.Background(), "product")
	if err != nil {
		t.Fatalf("GetEntitySchema() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/schema" {
		t.Errorf("path = %q, want /admin/schema", gotPath)
	}
	if gotType != "product" {
		t.Errorf("type query param = %q, want product", gotType)
	}
	if schema.Type != "product" || len(schema.Fields) != 2 {
		t.Errorf("schema = %+v, want type=product with 2 fields", schema)
	}
}

func TestClient_GetEntitySchema_UnknownType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown dynamic entity type: bogus"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetEntitySchema(context.Background(), "bogus")
	if err == nil {
		t.Fatal("GetEntitySchema() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("GetEntitySchema() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}

func TestClient_CreateEntityType(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody CreateEntityTypeInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(EntitySchema{
			Type: "widget",
			Fields: []SchemaField{
				{Name: "name", Kind: "string", Required: true},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	input := CreateEntityTypeInput{
		Type: "widget",
		Columns: []SchemaColumnInput{
			{Name: "name", Kind: "string", Required: true},
		},
		Indexes: []SchemaIndexInput{
			{Name: "widget_name_idx", Columns: []string{"name"}, Unique: true},
		},
	}
	schema, err := c.CreateEntityType(context.Background(), input)
	if err != nil {
		t.Fatalf("CreateEntityType() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/schema" {
		t.Errorf("path = %q, want /admin/schema", gotPath)
	}
	if gotBody.Type != "widget" {
		t.Errorf("request body type = %q, want widget", gotBody.Type)
	}
	if len(gotBody.Columns) != 1 || gotBody.Columns[0].Name != "name" {
		t.Errorf("request body columns = %+v, want [{name string true}]", gotBody.Columns)
	}
	if len(gotBody.Indexes) != 1 || !gotBody.Indexes[0].Unique {
		t.Errorf("request body indexes = %+v, want unique index", gotBody.Indexes)
	}
	if schema.Type != "widget" {
		t.Errorf("schema.Type = %q, want widget", schema.Type)
	}
}

func TestClient_CreateEntityType_Conflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "entity widget registered twice"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.CreateEntityType(context.Background(), CreateEntityTypeInput{Type: "widget"})
	if err == nil {
		t.Fatal("CreateEntityType() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("CreateEntityType() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusConflict {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusConflict)
	}
}

func TestClient_AddEntityFields(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody AddEntityFieldsInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EntitySchema{
			Type: "product",
			Fields: []SchemaField{
				{Name: "id", Kind: "string", Required: true},
				{Name: "sku", Kind: "string", Required: false},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	schema, err := c.AddEntityFields(context.Background(), "product", AddEntityFieldsInput{
		Columns: []SchemaColumnInput{{Name: "sku", Kind: "string"}},
	})
	if err != nil {
		t.Fatalf("AddEntityFields() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/schema/product/fields" {
		t.Errorf("path = %q, want /admin/schema/product/fields", gotPath)
	}
	if len(gotBody.Columns) != 1 || gotBody.Columns[0].Name != "sku" {
		t.Errorf("request body columns = %+v, want [{sku ...}]", gotBody.Columns)
	}
	if len(schema.Fields) != 2 {
		t.Errorf("schema.Fields = %+v, want 2 fields", schema.Fields)
	}
}

func TestClient_AddEntityFields_UnknownType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown dynamic entity type: bogus"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.AddEntityFields(context.Background(), "bogus", AddEntityFieldsInput{})
	if err == nil {
		t.Fatal("AddEntityFields() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("AddEntityFields() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotFound {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotFound)
	}
}

func TestClient_RenameEntityField(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"type": "product", "from": "sku", "to": "productSku"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	err := c.RenameEntityField(context.Background(), "product", "sku", "productSku")
	if err != nil {
		t.Fatalf("RenameEntityField() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/schema/product/rename-field" {
		t.Errorf("path = %q, want /admin/schema/product/rename-field", gotPath)
	}
	if gotBody["from"] != "sku" || gotBody["to"] != "productSku" {
		t.Errorf("request body = %+v, want from=sku to=productSku", gotBody)
	}
}

func TestClient_DropEntityField(t *testing.T) {
	var gotMethod, gotPath, gotConfirm string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotConfirm = r.URL.Query().Get("confirm")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"type": "product", "dropped": "sku"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	err := c.DropEntityField(context.Background(), "product", "sku")
	if err != nil {
		t.Fatalf("DropEntityField() unexpected error: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/admin/schema/product/fields/sku" {
		t.Errorf("path = %q, want /admin/schema/product/fields/sku", gotPath)
	}
	if gotConfirm != "sku" {
		t.Errorf("confirm query param = %q, want sku", gotConfirm)
	}
}

func TestClient_DropEntityField_ConfirmMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "confirmation required: pass ?confirm=sku"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	err := c.DropEntityField(context.Background(), "product", "sku")
	if err == nil {
		t.Fatal("DropEntityField() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("DropEntityField() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}

func TestClient_DeleteEntityType(t *testing.T) {
	var gotMethod, gotPath, gotConfirm string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotConfirm = r.URL.Query().Get("confirm")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"dropped": "widget"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	err := c.DeleteEntityType(context.Background(), "widget")
	if err != nil {
		t.Fatalf("DeleteEntityType() unexpected error: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/admin/schema/widget" {
		t.Errorf("path = %q, want /admin/schema/widget", gotPath)
	}
	if gotConfirm != "widget" {
		t.Errorf("confirm query param = %q, want widget", gotConfirm)
	}
}

func TestClient_DeleteEntityType_NotImplemented(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "dynamic entity operations unavailable"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	err := c.DeleteEntityType(context.Background(), "widget")
	if err == nil {
		t.Fatal("DeleteEntityType() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("DeleteEntityType() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}
