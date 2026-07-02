package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetMigrationStatus(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(MigrationStatus{
			Groups: []MigrationGroupStatus{
				{
					Name: "core",
					Applied: []MigrationInfo{
						{Name: "0001_outbox", Version: "202601010001", Group: "core", Comment: "outbox", Applied: true, AppliedAt: "2026-01-01T00:00:00Z"},
					},
					Pending: []MigrationInfo{
						{Name: "0002_widgets", Version: "202601020001", Group: "core", Comment: "widgets", Applied: false},
					},
				},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	status, err := c.GetMigrationStatus(context.Background())
	if err != nil {
		t.Fatalf("GetMigrationStatus() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/migrations" {
		t.Errorf("path = %q, want /admin/migrations", gotPath)
	}
	if len(status.Groups) != 1 || status.Groups[0].Name != "core" {
		t.Fatalf("status.Groups = %+v, want 1 group named core", status.Groups)
	}
	if len(status.Groups[0].Applied) != 1 || !status.Groups[0].Applied[0].Applied {
		t.Errorf("status.Groups[0].Applied = %+v, want 1 applied migration", status.Groups[0].Applied)
	}
	if len(status.Groups[0].Pending) != 1 || status.Groups[0].Pending[0].Applied {
		t.Errorf("status.Groups[0].Pending = %+v, want 1 pending migration", status.Groups[0].Pending)
	}
}

func TestClient_GetMigrationStatus_NotImplemented(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "migration status unavailable"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetMigrationStatus(context.Background())
	if err == nil {
		t.Fatal("GetMigrationStatus() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GetMigrationStatus() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_RunMigrations(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(MigrationJobHandle{JobID: "job-123"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	handle, err := c.RunMigrations(context.Background())
	if err != nil {
		t.Fatalf("RunMigrations() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/migrations/up" {
		t.Errorf("path = %q, want /admin/migrations/up", gotPath)
	}
	if handle.JobID != "job-123" {
		t.Errorf("handle.JobID = %q, want job-123", handle.JobID)
	}
}

func TestClient_RunMigrations_Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "schema admin not enabled"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RunMigrations(context.Background())
	if err == nil {
		t.Fatal("RunMigrations() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("RunMigrations() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusForbidden {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusForbidden)
	}
}

func TestClient_RollbackMigrations(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(MigrationJobHandle{JobID: "job-456"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	handle, err := c.RollbackMigrations(context.Background())
	if err != nil {
		t.Fatalf("RollbackMigrations() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/migrations/down" {
		t.Errorf("path = %q, want /admin/migrations/down", gotPath)
	}
	if handle.JobID != "job-456" {
		t.Errorf("handle.JobID = %q, want job-456", handle.JobID)
	}
}

func TestClient_RollbackMigrations_Conflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "another migration run is in progress"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RollbackMigrations(context.Background())
	if err == nil {
		t.Fatal("RollbackMigrations() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("RollbackMigrations() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusConflict {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusConflict)
	}
}

func TestClient_GetMigrationJob(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(MigrationJob{
			ID:        "job-123",
			Kind:      "up",
			State:     "done",
			Names:     []string{"0002_widgets"},
			StartedAt: "2026-07-01T00:00:00Z",
			EndedAt:   "2026-07-01T00:00:05Z",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	job, err := c.GetMigrationJob(context.Background(), "job-123")
	if err != nil {
		t.Fatalf("GetMigrationJob() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/migrations/jobs/job-123" {
		t.Errorf("path = %q, want /admin/migrations/jobs/job-123", gotPath)
	}
	if job.ID != "job-123" || job.State != "done" || job.Kind != "up" {
		t.Errorf("job = %+v, want id=job-123 state=done kind=up", job)
	}
	if len(job.Names) != 1 || job.Names[0] != "0002_widgets" {
		t.Errorf("job.Names = %v, want [0002_widgets]", job.Names)
	}
}

func TestClient_GetMigrationJob_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "no such job"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetMigrationJob(context.Background(), "missing")
	if err == nil {
		t.Fatal("GetMigrationJob() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GetMigrationJob() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotFound {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotFound)
	}
}

func TestClient_MigrationJobStreamPath(t *testing.T) {
	c := &Client{}
	got := c.MigrationJobStreamPath("job-123")
	want := "/migrations/jobs/job-123/stream"
	if got != want {
		t.Errorf("MigrationJobStreamPath() = %q, want %q", got, want)
	}
}

func TestClient_MigrationJobStreamPath_EscapesID(t *testing.T) {
	c := &Client{}
	got := c.MigrationJobStreamPath("job/with slash")
	want := "/migrations/jobs/job%2Fwith%20slash/stream"
	if got != want {
		t.Errorf("MigrationJobStreamPath() = %q, want %q", got, want)
	}
}

func TestClient_ScaffoldMigration(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody MigrationScaffoldInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(MigrationScaffold{
			Filename: "add_widget.go",
			Content:  "package migrations\n",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	input := MigrationScaffoldInput{
		Name:    "add_widget",
		Version: "202607020001",
		Up:      []string{"CREATE TABLE widgets (id text)"},
	}
	scaffold, err := c.ScaffoldMigration(context.Background(), input)
	if err != nil {
		t.Fatalf("ScaffoldMigration() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/migrations/scaffold" {
		t.Errorf("path = %q, want /admin/migrations/scaffold", gotPath)
	}
	if gotBody.Name != "add_widget" || gotBody.Version != "202607020001" {
		t.Errorf("request body = %+v, want name=add_widget version=202607020001", gotBody)
	}
	if len(gotBody.Up) != 1 {
		t.Errorf("request body up = %v, want 1 statement", gotBody.Up)
	}
	if scaffold.Filename != "add_widget.go" {
		t.Errorf("scaffold.Filename = %q, want add_widget.go", scaffold.Filename)
	}
}

func TestClient_ScaffoldMigration_Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "schema admin not enabled"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.ScaffoldMigration(context.Background(), MigrationScaffoldInput{Name: "x", Version: "1"})
	if err == nil {
		t.Fatal("ScaffoldMigration() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("ScaffoldMigration() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusForbidden {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusForbidden)
	}
}

func TestClient_GetSchemaDrift(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SchemaDrift{
			Entities: []DriftEntity{
				{
					Entity: "product", Table: "ds_products", Dynamic: true, InSync: false,
					Missing: []string{"sku"}, Extra: []string{"legacy_col"},
				},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	drift, err := c.GetSchemaDrift(context.Background())
	if err != nil {
		t.Fatalf("GetSchemaDrift() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/schema/drift" {
		t.Errorf("path = %q, want /admin/schema/drift", gotPath)
	}
	if len(drift.Entities) != 1 {
		t.Fatalf("drift.Entities = %+v, want 1 entity", drift.Entities)
	}
	e := drift.Entities[0]
	if e.Entity != "product" || e.InSync {
		t.Errorf("drift.Entities[0] = %+v, want entity=product inSync=false", e)
	}
	if len(e.Missing) != 1 || e.Missing[0] != "sku" {
		t.Errorf("drift.Entities[0].Missing = %v, want [sku]", e.Missing)
	}
	if len(e.Extra) != 1 || e.Extra[0] != "legacy_col" {
		t.Errorf("drift.Entities[0].Extra = %v, want [legacy_col]", e.Extra)
	}
}

func TestClient_GetSchemaDrift_NotImplemented(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "drift not available (no relational store)"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetSchemaDrift(context.Background())
	if err == nil {
		t.Fatal("GetSchemaDrift() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("GetSchemaDrift() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_RunDDL(t *testing.T) {
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
		_ = json.NewEncoder(w).Encode(DDLResult{OK: true, Executed: "ALTER TABLE ds_products ADD COLUMN sku text"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.RunDDL(context.Background(), "ALTER TABLE ds_products ADD COLUMN sku text")
	if err != nil {
		t.Fatalf("RunDDL() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/schema/ddl" {
		t.Errorf("path = %q, want /admin/schema/ddl", gotPath)
	}
	if gotBody["sql"] != "ALTER TABLE ds_products ADD COLUMN sku text" {
		t.Errorf("request body sql = %q, want the DDL text", gotBody["sql"])
	}
	if !result.OK {
		t.Errorf("result.OK = %v, want true", result.OK)
	}
}

func TestClient_RunDDL_Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "schema admin not enabled"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RunDDL(context.Background(), "DROP TABLE ds_products")
	if err == nil {
		t.Fatal("RunDDL() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("RunDDL() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusForbidden {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusForbidden)
	}
}

func TestClient_RunDDL_BadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "syntax error at or near \"FOO\""})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.RunDDL(context.Background(), "FOO BAR")
	if err == nil {
		t.Fatal("RunDDL() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("RunDDL() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}
