package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_ListFiles(t *testing.T) {
	var gotMethod, gotPath, gotParent, gotLimit, gotOffset string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotParent = r.URL.Query().Get("parent")
		gotLimit = r.URL.Query().Get("limit")
		gotOffset = r.URL.Query().Get("offset")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(FileListPage{
			Items: []FileNode{
				{ID: "f1", Name: "report.pdf", Kind: "file", Size: 1024, ContentType: "application/pdf", ParentID: "root"},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	page, err := c.ListFiles(context.Background(), ListFilesParams{
		Parent: "root",
		Limit:  10,
		Offset: 5,
	})
	if err != nil {
		t.Fatalf("ListFiles() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/files" {
		t.Errorf("path = %q, want /admin/files", gotPath)
	}
	if gotParent != "root" {
		t.Errorf("query param parent = %q, want %q", gotParent, "root")
	}
	if gotLimit != "10" {
		t.Errorf("query param limit = %q, want %q", gotLimit, "10")
	}
	if gotOffset != "5" {
		t.Errorf("query param offset = %q, want %q", gotOffset, "5")
	}

	if len(page.Items) != 1 {
		t.Fatalf("len(page.Items) = %d, want 1", len(page.Items))
	}
	if page.Items[0].ID != "f1" || page.Items[0].Kind != "file" {
		t.Errorf("page.Items[0] = %+v, want id=f1 kind=file", page.Items[0])
	}
}

func TestClient_ListFiles_OmitsOptionalQueryParams(t *testing.T) {
	var gotRawQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(FileListPage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.ListFiles(context.Background(), ListFilesParams{}); err != nil {
		t.Fatalf("ListFiles() unexpected error: %v", err)
	}

	if gotRawQuery != "" {
		t.Errorf("raw query = %q, want empty", gotRawQuery)
	}
}

func TestClient_GetFile(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(FileNode{
			ID:   "f1",
			Name: "report.pdf",
			Kind: "file",
			Size: 2048,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	node, err := c.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatalf("GetFile() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/files/f1" {
		t.Errorf("path = %q, want /admin/files/f1", gotPath)
	}
	if node.ID != "f1" || node.Size != 2048 {
		t.Errorf("node = %+v, want id=f1 size=2048", node)
	}
}

func TestClient_CreateFolder(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody CreateFolderInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(FileNode{
			ID:       "folder-1",
			Name:     gotBody.Name,
			Kind:     "folder",
			ParentID: gotBody.ParentID,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	node, err := c.CreateFolder(context.Background(), CreateFolderInput{
		ParentID: "root",
		Name:     "reports",
	})
	if err != nil {
		t.Fatalf("CreateFolder() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/files/folder" {
		t.Errorf("path = %q, want /admin/files/folder", gotPath)
	}
	if gotBody.Name != "reports" {
		t.Errorf("request body name = %q, want %q", gotBody.Name, "reports")
	}
	if gotBody.ParentID != "root" {
		t.Errorf("request body parentId = %q, want %q", gotBody.ParentID, "root")
	}
	if node.ID != "folder-1" || node.Kind != "folder" {
		t.Errorf("node = %+v, want id=folder-1 kind=folder", node)
	}
}

func TestClient_UploadFile(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody uploadFileWireRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(FileNode{
			ID:          "f-new",
			Name:        gotBody.Name,
			Kind:        "file",
			ContentType: gotBody.ContentType,
			ParentID:    gotBody.ParentID,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	node, err := c.UploadFile(context.Background(), UploadFileInput{
		ParentID:    "root",
		Name:        "hello.txt",
		ContentType: "text/plain",
		Data:        []byte("hello world"),
	})
	if err != nil {
		t.Fatalf("UploadFile() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/files" {
		t.Errorf("path = %q, want /admin/files", gotPath)
	}
	if gotBody.Name != "hello.txt" {
		t.Errorf("request body name = %q, want %q", gotBody.Name, "hello.txt")
	}
	if gotBody.ContentType != "text/plain" {
		t.Errorf("request body contentType = %q, want %q", gotBody.ContentType, "text/plain")
	}
	wantB64 := base64.StdEncoding.EncodeToString([]byte("hello world"))
	if gotBody.DataBase64 != wantB64 {
		t.Errorf("request body dataBase64 = %q, want %q", gotBody.DataBase64, wantB64)
	}
	if node.ID != "f-new" {
		t.Errorf("node.ID = %q, want %q", node.ID, "f-new")
	}
}

func TestClient_DeleteFile(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if err := c.DeleteFile(context.Background(), "f1"); err != nil {
		t.Fatalf("DeleteFile() unexpected error: %v", err)
	}

	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if gotPath != "/admin/files/f1" {
		t.Errorf("path = %q, want /admin/files/f1", gotPath)
	}
}

func TestClient_DownloadFile(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotTenant, gotVersion string
	const wantBody = "the quick brown fox"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotTenant = r.Header.Get("X-Tenant-ID")
		gotVersion = r.Header.Get("X-Fabriq-Api-Version")

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Disposition", `attachment; filename="fox.txt"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(wantBody))
	}))
	defer srv.Close()

	c := testClient(t, srv, "acme")

	rc, filename, err := c.DownloadFile(context.Background(), "f1")
	if err != nil {
		t.Fatalf("DownloadFile() unexpected error: %v", err)
	}
	defer rc.Close()

	gotBody, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read downloaded body: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/files/f1/content" {
		t.Errorf("path = %q, want /admin/files/f1/content", gotPath)
	}
	if gotAuth != "Bearer fq_testkey" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer fq_testkey")
	}
	if gotTenant != "acme" {
		t.Errorf("X-Tenant-ID header = %q, want %q", gotTenant, "acme")
	}
	if gotVersion != "3" {
		t.Errorf("X-Fabriq-Api-Version header = %q, want %q", gotVersion, "3")
	}
	if string(gotBody) != wantBody {
		t.Errorf("downloaded body = %q, want %q", string(gotBody), wantBody)
	}
	if filename != "fox.txt" {
		t.Errorf("filename = %q, want %q", filename, "fox.txt")
	}
}

func TestClient_DownloadFile_FallsBackToIDWhenNoContentDisposition(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	rc, filename, err := c.DownloadFile(context.Background(), "f1")
	if err != nil {
		t.Fatalf("DownloadFile() unexpected error: %v", err)
	}
	defer rc.Close()

	if filename != "f1" {
		t.Errorf("filename = %q, want fallback to id %q", filename, "f1")
	}
}

func TestClient_DownloadFile_NonSuccessReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":"not_found","message":"node not found"}}`))
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	rc, filename, err := c.DownloadFile(context.Background(), "missing")
	if err == nil {
		t.Fatal("DownloadFile() expected error, got nil")
	}
	if rc != nil {
		t.Errorf("DownloadFile() body = %v, want nil on error", rc)
	}
	if filename != "" {
		t.Errorf("DownloadFile() filename = %q, want empty on error", filename)
	}

	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotFound {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotFound)
	}
	if apiErr.Message != "node not found" {
		t.Errorf("apiErr.Message = %q, want %q", apiErr.Message, "node not found")
	}
}
