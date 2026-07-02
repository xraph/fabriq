package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetDigestMap(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(DigestMap{
			RootID: "digest:2:tenant",
			Nodes: []DigestNode{
				{
					ID:          "digest:2:tenant",
					Level:       2,
					Kind:        "tenant",
					ContentHash: "abc123",
					SemHash:     "def456",
				},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	m, err := c.GetDigestMap(context.Background())
	if err != nil {
		t.Fatalf("GetDigestMap() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/distill/map" {
		t.Errorf("path = %q, want /admin/distill/map", gotPath)
	}
	if m.RootID != "digest:2:tenant" {
		t.Errorf("m.RootID = %q, want %q", m.RootID, "digest:2:tenant")
	}
	if len(m.Nodes) != 1 {
		t.Fatalf("len(m.Nodes) = %d, want 1", len(m.Nodes))
	}
	if m.Nodes[0].Level != 2 || m.Nodes[0].Kind != "tenant" {
		t.Errorf("m.Nodes[0] = %+v, want level=2 kind=tenant", m.Nodes[0])
	}
}

func TestClient_GetDigestMap_EmptyNodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(DigestMap{
			RootID: "digest:2:tenant",
			Nodes:  []DigestNode{},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	m, err := c.GetDigestMap(context.Background())
	if err != nil {
		t.Fatalf("GetDigestMap() unexpected error: %v", err)
	}
	if len(m.Nodes) != 0 {
		t.Errorf("len(m.Nodes) = %d, want 0", len(m.Nodes))
	}
}

func TestClient_GetDigestNode(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(DigestView{
			Node: DigestNode{
				ID:    "digest:1:scope:region-us",
				Level: 1,
				Kind:  "scope",
				Scope: "region-us",
			},
			Summary: "US region summary",
			Children: []DigestChild{
				{ID: "digest:0:product:42", Kind: "entity", Summary: "widget"},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	view, err := c.GetDigestNode(context.Background(), "digest:1:scope:region-us")
	if err != nil {
		t.Fatalf("GetDigestNode() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	wantPath := "/admin/distill/node/digest:1:scope:region-us"
	if gotPath != wantPath {
		t.Errorf("path = %q, want %q", gotPath, wantPath)
	}
	if view.Node.ID != "digest:1:scope:region-us" {
		t.Errorf("view.Node.ID = %q, want %q", view.Node.ID, "digest:1:scope:region-us")
	}
	if view.Summary != "US region summary" {
		t.Errorf("view.Summary = %q, want %q", view.Summary, "US region summary")
	}
	if len(view.Children) != 1 || view.Children[0].ID != "digest:0:product:42" {
		t.Errorf("view.Children = %+v, want one child digest:0:product:42", view.Children)
	}
}

func TestClient_GetDigestNode_NotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "distillation plane not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetDigestNode(context.Background(), "digest:2:tenant")
	if err == nil {
		t.Fatalf("GetDigestNode() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestEncodeDigestID(t *testing.T) {
	got := encodeDigestID("digest:0:product:foo bar")
	want := "digest:0:product:foo%20bar"
	if got != want {
		t.Errorf("encodeDigestID() = %q, want %q", got, want)
	}
}
