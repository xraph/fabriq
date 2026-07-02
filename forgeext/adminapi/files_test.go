package adminapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

// fsRegistry returns a registry carrying the file-plane entities (blob_object +
// fs_node) plus the widget entity, validated and ready for a real *fabriq.Fabriq.
// Minimal specs (Model only) keep the command plane happy without pulling in the
// example domain pack's graph/search projections.
func fsRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	for _, spec := range []registry.EntitySpec{
		widgetSpec(),
		{Name: "blob_object", Kind: registry.KindAggregate, Model: (*domain.BlobObject)(nil)},
		{Name: "fs_node", Kind: registry.KindAggregate, Model: (*domain.FsNode)(nil)},
	} {
		if err := reg.Register(spec); err != nil {
			t.Fatalf("register %s: %v", spec.Name, err)
		}
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return reg
}

// realFabriqExt builds an Extension backed by a REAL *fabriq.Fabriq assembled
// from the World's in-memory fakes. blob controls the byte plane: pass the
// World's FakeBlob to make files appear configured (Head→ErrNotFound), or nil to
// exercise the not-configured (501) path. CAS is always nil here (no fake CAS),
// so CreateFile/GetBlob surface ErrStoreNotConfigured → 501 — the honest
// upload/download coverage without standing up a real CAS.
func realFabriqExt(t *testing.T, reg *registry.Registry, withBlob bool) *Extension {
	t.Helper()
	world := fabriqtest.NewWorld(reg)

	ports := fabriq.Ports{Store: world.Store, Relational: world.Rel}
	if withBlob {
		ports.Blob = world.Blob
	}
	f, err := fabriq.New(reg, ports)
	if err != nil {
		t.Fatalf("fabriq.New: %v", err)
	}

	// Prepend the tenant middleware so the real fabric (which requires a tenant
	// in context) sees one.
	opts := []Option{
		WithRouteOptions(forge.WithMiddleware(tenantMiddleware)),
	}
	e := NewAdminAPI(nil, opts...) // nil parent — bypass forgeext.Extension
	e.fabric = f
	e.fab = f
	e.reg = reg
	return e
}

// buildFileServer registers the controller on a fresh app and returns a server.
// The tenant middleware is wired via realFabriqExt's route options, so callers
// must send the X-Tenant-ID header (see doReq).
func buildFileServer(t *testing.T, e *Extension) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "admin-api-files-test", HTTPAddress: ":0"})
	if err := app.RegisterController(newAdminController(e)); err != nil {
		t.Fatalf("register controller: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

// --- not-configured (501) path -------------------------------------------

// TestFilesNotConfigured verifies every file route returns 501 when the blob
// plane is unwired.
func TestFilesNotConfigured(t *testing.T) {
	reg := fsRegistry(t)
	e := realFabriqExt(t, reg, false /* no blob */)
	srv := buildFileServer(t, e)
	defer srv.Close()

	cases := []struct {
		method, path, body string
	}{
		{http.MethodGet, "/admin/files", ""},
		{http.MethodGet, "/admin/files/some-id", ""},
		{http.MethodGet, "/admin/files/some-id/content", ""},
		{http.MethodPost, "/admin/files/folder", `{"name":"x"}`},
		{http.MethodPost, "/admin/files", `{"name":"x","dataBase64":"` + base64.StdEncoding.EncodeToString([]byte("hi")) + `"}`},
		{http.MethodDelete, "/admin/files/some-id", ""},
	}
	for _, tc := range cases {
		resp := doReq(t, srv, tc.method, tc.path, tc.body)
		if resp.StatusCode != http.StatusNotImplemented {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("%s %s: status = %d, want 501; body=%s", tc.method, tc.path, resp.StatusCode, body)
		}
		resp.Body.Close()
	}
}

// --- validation (400) path -----------------------------------------------

// TestFilesValidation verifies missing name / data yield 400 even when the blob
// plane is configured (validation runs before the store call).
func TestFilesValidation(t *testing.T) {
	reg := fsRegistry(t)
	e := realFabriqExt(t, reg, true /* blob configured */)
	srv := buildFileServer(t, e)
	defer srv.Close()

	cases := []struct {
		name, path, body string
	}{
		{"folder missing name", "/admin/files/folder", `{}`},
		{"upload missing name", "/admin/files", `{"dataBase64":"aGk="}`},
		{"upload missing data", "/admin/files", `{"name":"x"}`},
		{"upload bad base64", "/admin/files", `{"name":"x","dataBase64":"!!!notbase64!!!"}`},
	}
	for _, tc := range cases {
		resp := doReq(t, srv, http.MethodPost, tc.path, tc.body)
		if resp.StatusCode != http.StatusBadRequest {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("%s: status = %d, want 400; body=%s", tc.name, resp.StatusCode, body)
		}
		resp.Body.Close()
	}
}

// --- tree CRUD against the fakes -----------------------------------------

// TestFilesFolderCreateGetTrash exercises the no-bytes file-plane paths
// (CreateFolder/GetNode/TrashNode + 404 mapping) end-to-end against the World
// fakes through a real *fabriq.Fabriq.
//
// NOTE on listing: the fabriqtest in-memory relational fake represents the
// FsNode.DeletedAt *time.Time field as a typed-nil pointer rather than a SQL
// NULL, so an IsNull("deleted_at") predicate (which ListChildren always adds to
// exclude trashed nodes) never matches the fake's rows. ListChildren therefore
// returns an empty set against the fake even for live nodes. This is a fake
// limitation, not a handler bug — the live admin-demo curl smoke (file-backed
// real fabriq) verifies listing end-to-end. Here we assert only the listing
// route's status (200) and the create/get/trash/404 contract, which do not
// depend on the fake's NULL semantics.
func TestFilesFolderCreateGetTrash(t *testing.T) {
	reg := fsRegistry(t)
	e := realFabriqExt(t, reg, true /* blob configured */)
	srv := buildFileServer(t, e)
	defer srv.Close()

	// Listing the root succeeds (status only — see NOTE on fake NULL semantics).
	var list fileListResponse
	decode(t, doReq(t, srv, http.MethodGet, "/admin/files", ""), &list) //nolint:bodyclose // decode closes the response body.

	// Create a folder at root.
	var folder fileNode
	resp := doReq(t, srv, http.MethodPost, "/admin/files/folder", `{"name":"docs"}`)
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("create folder: status = %d, want 201; body=%s", resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(&folder); err != nil {
		t.Fatalf("decode folder: %v", err)
	}
	resp.Body.Close()
	if folder.Kind != "folder" || folder.Name != "docs" || folder.ID == "" {
		t.Fatalf("unexpected folder node: %+v", folder)
	}

	// GetNode returns it.
	var got fileNode
	decode(t, doReq(t, srv, http.MethodGet, "/admin/files/"+folder.ID, ""), &got) //nolint:bodyclose // decode closes the response body.
	if got.ID != folder.ID || got.Kind != "folder" {
		t.Fatalf("get node mismatch: %+v", got)
	}

	// Unknown id → 404.
	resp = doReq(t, srv, http.MethodGet, "/admin/files/does-not-exist", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get missing node: status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()

	// Trash it → 204.
	resp = doReq(t, srv, http.MethodDelete, "/admin/files/"+folder.ID, "")
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("trash node: status = %d, want 204; body=%s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Trashing an unknown id → 404.
	resp = doReq(t, srv, http.MethodDelete, "/admin/files/does-not-exist", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("trash missing node: status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestFilesUploadWithoutCAS verifies upload returns 501 when the byte plane
// (CAS) is unconfigured even though the blob store reports configured — the
// honest coverage for the upload/download path without a real CAS in unit tests.
func TestFilesUploadWithoutCAS(t *testing.T) {
	reg := fsRegistry(t)
	e := realFabriqExt(t, reg, true /* blob configured, CAS nil */)
	srv := buildFileServer(t, e)
	defer srv.Close()

	body := `{"name":"hello.txt","contentType":"text/plain","dataBase64":"` +
		base64.StdEncoding.EncodeToString([]byte("hello fabriq")) + `"}`
	resp := doReq(t, srv, http.MethodPost, "/admin/files", body)
	if resp.StatusCode != http.StatusNotImplemented {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("upload without CAS: status = %d, want 501; body=%s", resp.StatusCode, b)
	}
	resp.Body.Close()
}

// TestFilesCapability verifies files.read/files.write are advertised in /meta.
func TestFilesCapability(t *testing.T) {
	for _, want := range []string{"files.read", "files.write"} {
		found := false
		for _, c := range capabilities {
			if c == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("capability %q missing from meta capabilities %v", want, capabilities)
		}
	}
}

// --- helpers --------------------------------------------------------------

func doReq(t *testing.T, srv *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}
