package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_Recall(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody RecallRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(RecallPack{
			Items: []RecallItem{
				{
					Entity: "product",
					ID:     "42",
					Row:    json.RawMessage(`{"name":"gizmo"}`),
					Score:  0.91,
					Source: []string{"vector", "search"},
					Tokens: 12,
				},
			},
			Omitted:  1,
			Tokens:   12,
			Warnings: []string{"graph channel skipped"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	pack, err := c.Recall(context.Background(), RecallRequest{
		Query:    "gizmos",
		Entities: []string{"product"},
		Budget:   500,
		K:        5,
		Hops:     2,
	})
	if err != nil {
		t.Fatalf("Recall() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/recall" {
		t.Errorf("path = %q, want /admin/recall", gotPath)
	}
	if gotBody.Query != "gizmos" {
		t.Errorf("request body query = %q, want %q", gotBody.Query, "gizmos")
	}
	if len(gotBody.Entities) != 1 || gotBody.Entities[0] != "product" {
		t.Errorf("request body entities = %v, want [product]", gotBody.Entities)
	}
	if gotBody.Budget != 500 || gotBody.K != 5 || gotBody.Hops != 2 {
		t.Errorf("request body budget/k/hops = %d/%d/%d, want 500/5/2", gotBody.Budget, gotBody.K, gotBody.Hops)
	}

	if len(pack.Items) != 1 {
		t.Fatalf("len(pack.Items) = %d, want 1", len(pack.Items))
	}
	item := pack.Items[0]
	if item.Entity != "product" || item.ID != "42" || item.Score != 0.91 {
		t.Errorf("pack.Items[0] = %+v, want entity=product id=42 score=0.91", item)
	}
	if len(item.Source) != 2 || item.Source[0] != "vector" {
		t.Errorf("pack.Items[0].Source = %v, want [vector search]", item.Source)
	}
	var row map[string]any
	if jsonErr := json.Unmarshal(item.Row, &row); jsonErr != nil {
		t.Fatalf("decode item.Row: %v", jsonErr)
	}
	if row["name"] != "gizmo" {
		t.Errorf("item.Row[name] = %v, want gizmo", row["name"])
	}
	if pack.Omitted != 1 || pack.Tokens != 12 {
		t.Errorf("pack.Omitted/Tokens = %d/%d, want 1/12", pack.Omitted, pack.Tokens)
	}
	if len(pack.Warnings) != 1 || pack.Warnings[0] != "graph channel skipped" {
		t.Errorf("pack.Warnings = %v, want [graph channel skipped]", pack.Warnings)
	}
}

func TestClient_Recall_NotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "recall not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.Recall(context.Background(), RecallRequest{Query: "gizmos"})
	if err == nil {
		t.Fatalf("Recall() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_Recall_EmptyQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "body field 'query' is required"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.Recall(context.Background(), RecallRequest{})
	if err == nil {
		t.Fatalf("Recall() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}

func TestClient_GetWritePolicy(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(WritePolicy{
			Allow: map[string][]string{"product": {"create", "update"}},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	policy, err := c.GetWritePolicy(context.Background())
	if err != nil {
		t.Fatalf("GetWritePolicy() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/agent/write-policy" {
		t.Errorf("path = %q, want /admin/agent/write-policy", gotPath)
	}
	ops, ok := policy.Allow["product"]
	if !ok || len(ops) != 2 || ops[0] != "create" || ops[1] != "update" {
		t.Errorf("policy.Allow[product] = %v, want [create update]", ops)
	}
}

func TestClient_GetWritePolicy_EmptyDeniesAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(WritePolicy{Allow: map[string][]string{}})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	policy, err := c.GetWritePolicy(context.Background())
	if err != nil {
		t.Fatalf("GetWritePolicy() unexpected error: %v", err)
	}
	if len(policy.Allow) != 0 {
		t.Errorf("policy.Allow = %v, want empty", policy.Allow)
	}
}

func TestClient_Remember(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody RememberInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(rememberResponse{
			Result: CommandResult{AggID: "agg-1", Version: 1, EventID: "evt-1"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	res, err := c.Remember(context.Background(), RememberInput{
		Entity:  "product",
		Op:      CommandOpCreate,
		Payload: map[string]any{"name": "gizmo"},
	})
	if err != nil {
		t.Fatalf("Remember() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/agent/remember" {
		t.Errorf("path = %q, want /admin/agent/remember", gotPath)
	}
	if gotBody.Entity != "product" || gotBody.Op != CommandOpCreate {
		t.Errorf("request body = %+v, want entity=product op=create", gotBody)
	}
	if gotBody.Payload["name"] != "gizmo" {
		t.Errorf("request body payload[name] = %v, want gizmo", gotBody.Payload["name"])
	}
	if res.AggID != "agg-1" || res.Version != 1 || res.EventID != "evt-1" {
		t.Errorf("res = %+v, want aggId=agg-1 version=1 eventId=evt-1", res)
	}
}

func TestClient_Remember_NotAllowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "write not allowed: product.delete",
			"code":  "not_allowed",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.Remember(context.Background(), RememberInput{
		Entity: "product",
		Op:     CommandOpDelete,
		AggID:  "agg-1",
	})
	if err == nil {
		t.Fatalf("Remember() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusForbidden {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusForbidden)
	}
	if apiErr.Code != "not_allowed" {
		t.Errorf("Code = %q, want %q", apiErr.Code, "not_allowed")
	}
}
