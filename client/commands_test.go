package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_ExecCommand(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody CommandInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(commandResponse{
			Result: CommandResult{AggID: "agg-1", Version: 1, EventID: "evt-1"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	ev := int64(3)
	res, err := c.ExecCommand(context.Background(), CommandInput{
		Entity:          "product",
		Op:              CommandOpUpdate,
		AggID:           "agg-1",
		Payload:         map[string]any{"name": "gizmo"},
		ExpectedVersion: &ev,
	})
	if err != nil {
		t.Fatalf("ExecCommand() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/commands" {
		t.Errorf("path = %q, want /admin/commands", gotPath)
	}
	if gotBody.Entity != "product" || gotBody.Op != CommandOpUpdate || gotBody.AggID != "agg-1" {
		t.Errorf("request body = %+v, want entity=product op=update aggId=agg-1", gotBody)
	}
	if gotBody.ExpectedVersion == nil || *gotBody.ExpectedVersion != 3 {
		t.Errorf("request body ExpectedVersion = %v, want 3", gotBody.ExpectedVersion)
	}
	if gotBody.Payload["name"] != "gizmo" {
		t.Errorf("request body payload[name] = %v, want gizmo", gotBody.Payload["name"])
	}
	if res.AggID != "agg-1" || res.Version != 1 || res.EventID != "evt-1" {
		t.Errorf("res = %+v, want aggId=agg-1 version=1 eventId=evt-1", res)
	}
}

func TestClient_ExecCommand_OmitsOptionalFields(t *testing.T) {
	var gotRawBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Errorf("server: read request body: %v", readErr)
		}
		gotRawBody = string(raw)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(commandResponse{
			Result: CommandResult{AggID: "agg-new", Version: 1, EventID: "evt-1"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.ExecCommand(context.Background(), CommandInput{
		Entity: "product",
		Op:     CommandOpCreate,
	})
	if err != nil {
		t.Fatalf("ExecCommand() unexpected error: %v", err)
	}

	var decoded map[string]any
	if jsonErr := json.Unmarshal([]byte(gotRawBody), &decoded); jsonErr != nil {
		t.Fatalf("decode raw body: %v", jsonErr)
	}
	if _, ok := decoded["aggId"]; ok {
		t.Errorf("request body should omit aggId when empty, got %v", decoded)
	}
	if _, ok := decoded["expectedVersion"]; ok {
		t.Errorf("request body should omit expectedVersion when nil, got %v", decoded)
	}
	if _, ok := decoded["payload"]; ok {
		t.Errorf("request body should omit payload when nil, got %v", decoded)
	}
}

func TestClient_ExecCommand_Conflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": "version_conflict", "message": "version mismatch"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	ev := int64(1)
	_, err := c.ExecCommand(context.Background(), CommandInput{
		Entity:          "product",
		Op:              CommandOpUpdate,
		AggID:           "agg-1",
		ExpectedVersion: &ev,
	})
	if err == nil {
		t.Fatalf("ExecCommand() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusConflict {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusConflict)
	}
	if apiErr.Code != "version_conflict" {
		t.Errorf("Code = %q, want %q", apiErr.Code, "version_conflict")
	}
}

func TestClient_ExecBatch(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody commandBatchRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(commandBatchResponse{
			Results: []CommandResult{
				{AggID: "agg-1", Version: 1, EventID: "evt-1"},
				{AggID: "agg-2", Version: 1, EventID: "evt-2"},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	results, err := c.ExecBatch(context.Background(), []CommandInput{
		{Entity: "product", Op: CommandOpCreate, Payload: map[string]any{"name": "a"}},
		{Entity: "product", Op: CommandOpCreate, Payload: map[string]any{"name": "b"}},
	})
	if err != nil {
		t.Fatalf("ExecBatch() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/commands/batch" {
		t.Errorf("path = %q, want /admin/commands/batch", gotPath)
	}
	if len(gotBody.Commands) != 2 {
		t.Fatalf("len(gotBody.Commands) = %d, want 2", len(gotBody.Commands))
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].AggID != "agg-1" || results[1].AggID != "agg-2" {
		t.Errorf("results = %+v, want agg-1, agg-2", results)
	}
}

func TestClient_ExecBatch_TooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": "bad_request", "message": "batch too large: 101 commands (max 100)"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.ExecBatch(context.Background(), []CommandInput{{Entity: "product", Op: CommandOpCreate}})
	if err == nil {
		t.Fatalf("ExecBatch() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}
