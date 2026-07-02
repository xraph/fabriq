package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetMeta(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(Meta{
			Name:         "fabriq-admin-api",
			Version:      "v1.2.3",
			Capabilities: []string{"entities.read", "entities.write"},
			Tenant:       "acme",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	meta, err := c.GetMeta(context.Background())
	if err != nil {
		t.Fatalf("GetMeta() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/meta" {
		t.Errorf("path = %q, want /admin/meta", gotPath)
	}
	if meta.Name != "fabriq-admin-api" || meta.Version != "v1.2.3" {
		t.Errorf("meta = %+v, want name=fabriq-admin-api version=v1.2.3", meta)
	}
	if len(meta.Capabilities) != 2 || meta.Capabilities[0] != "entities.read" {
		t.Errorf("meta.Capabilities = %v, want [entities.read entities.write]", meta.Capabilities)
	}
	if meta.Tenant != "acme" {
		t.Errorf("meta.Tenant = %q, want acme", meta.Tenant)
	}
}

func TestClient_GetInstanceCapabilities(t *testing.T) {
	var gotMethod, gotPath, gotQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"capabilities": InstanceCapabilities{
				Relational: true,
				Graph:      true,
				Vector:     false,
				Spatial:    false,
				Search:     true,
				CRDT:       false,
				Files:      true,
				Distill:    false,
				Timeseries: false,
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	caps, err := c.GetInstanceCapabilities(context.Background())
	if err != nil {
		t.Fatalf("GetInstanceCapabilities() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/capabilities" {
		t.Errorf("path = %q, want /admin/capabilities", gotPath)
	}
	if gotQuery != "" {
		t.Errorf("query = %q, want empty (instance-level call omits ?type=)", gotQuery)
	}
	if !caps.Relational || !caps.Graph || caps.Vector || !caps.Search || !caps.Files {
		t.Errorf("caps = %+v, want relational=true graph=true vector=false search=true files=true", caps)
	}
}

func TestClient_GetTypeCapabilities(t *testing.T) {
	var gotMethod, gotPath, gotType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.URL.Query().Get("type")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(TypeCapabilitiesResult{
			Type: "product",
			Capabilities: TypeCapabilities{
				Relational: true,
				Vector:     true,
				Search:     true,
				Spatial:    false,
				CRDT:       false,
				Graph:      false,
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.GetTypeCapabilities(context.Background(), "product")
	if err != nil {
		t.Fatalf("GetTypeCapabilities() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/capabilities" {
		t.Errorf("path = %q, want /admin/capabilities", gotPath)
	}
	if gotType != "product" {
		t.Errorf("type query param = %q, want product", gotType)
	}
	if result.Type != "product" {
		t.Errorf("result.Type = %q, want product", result.Type)
	}
	if !result.Capabilities.Vector || !result.Capabilities.Search {
		t.Errorf("result.Capabilities = %+v, want vector=true search=true", result.Capabilities)
	}
}

func TestClient_GetTypeCapabilities_UnknownType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "unknown dynamic entity type: bogus"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetTypeCapabilities(context.Background(), "bogus")
	if err == nil {
		t.Fatal("GetTypeCapabilities() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("GetTypeCapabilities() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}
