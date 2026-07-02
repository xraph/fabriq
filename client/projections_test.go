package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetProjections(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ProjectionsInfo{
			Projections: []ProjectionStatus{
				{
					Name:         "graph",
					Status:       "live",
					ModelVersion: 1,
					EventVersion: "01H",
					TargetName:   "",
				},
				{
					Name:         "search",
					Status:       "building",
					ModelVersion: 2,
					EventVersion: "01Z",
					TargetName:   "tenant_1_v2",
				},
			},
			Backlog: 4,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	info, err := c.GetProjections(context.Background())
	if err != nil {
		t.Fatalf("GetProjections() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/projections" {
		t.Errorf("path = %q, want /admin/projections", gotPath)
	}
	if len(info.Projections) != 2 {
		t.Fatalf("len(info.Projections) = %d, want 2", len(info.Projections))
	}
	if info.Projections[0].Name != "graph" || info.Projections[0].Status != "live" {
		t.Errorf("info.Projections[0] = %+v, want name=graph status=live", info.Projections[0])
	}
	if info.Projections[1].TargetName != "tenant_1_v2" {
		t.Errorf("info.Projections[1].TargetName = %q, want %q", info.Projections[1].TargetName, "tenant_1_v2")
	}
	if info.Backlog != 4 {
		t.Errorf("info.Backlog = %d, want 4", info.Backlog)
	}
}

func TestClient_GetProjections_NotAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "projection bookkeeping not available (no Postgres store)",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetProjections(context.Background())
	if err == nil {
		t.Fatal("GetProjections() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GetProjections() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_ProjectionReconcile(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ProjectionReconcileResult{
			Projection: "graph",
			Repaired:   true,
			DriftCount: 1,
			Drifts: []ProjectionDrift{
				{Entity: "widget", AggID: "42", TruthVersion: 3, ProjectedVersion: 2},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.ProjectionReconcile(context.Background(), "graph", true)
	if err != nil {
		t.Fatalf("ProjectionReconcile() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/projections/reconcile" {
		t.Errorf("path = %q, want /admin/projections/reconcile", gotPath)
	}
	if gotBody["projection"] != "graph" {
		t.Errorf("request body projection = %v, want %q", gotBody["projection"], "graph")
	}
	if gotBody["repair"] != true {
		t.Errorf("request body repair = %v, want true", gotBody["repair"])
	}
	if result.Projection != "graph" || !result.Repaired {
		t.Errorf("result = %+v, want projection=graph repaired=true", result)
	}
	if result.DriftCount != 1 || len(result.Drifts) != 1 {
		t.Fatalf("result.DriftCount/Drifts = %d/%v, want 1/[1 item]", result.DriftCount, result.Drifts)
	}
	if result.Drifts[0].Entity != "widget" || result.Drifts[0].AggID != "42" {
		t.Errorf("result.Drifts[0] = %+v, want entity=widget aggId=42", result.Drifts[0])
	}
}

func TestClient_ProjectionReconcile_NoRepair(t *testing.T) {
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ProjectionReconcileResult{Projection: "search"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.ProjectionReconcile(context.Background(), "search", false); err != nil {
		t.Fatalf("ProjectionReconcile() unexpected error: %v", err)
	}

	if gotBody["repair"] != false {
		t.Errorf("request body repair = %v, want false", gotBody["repair"])
	}
}

func TestClient_ProjectionRebuild(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ProjectionRebuildResult{
			Projection: "search",
			OldTarget:  "tenant_1_v1",
			NewTarget:  "tenant_1_v2",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.ProjectionRebuild(context.Background(), "search")
	if err != nil {
		t.Fatalf("ProjectionRebuild() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/projections/rebuild" {
		t.Errorf("path = %q, want /admin/projections/rebuild", gotPath)
	}
	if gotBody["projection"] != "search" {
		t.Errorf("request body projection = %v, want %q", gotBody["projection"], "search")
	}
	if result.OldTarget != "tenant_1_v1" || result.NewTarget != "tenant_1_v2" {
		t.Errorf("result = %+v, want oldTarget=tenant_1_v1 newTarget=tenant_1_v2", result)
	}
}
