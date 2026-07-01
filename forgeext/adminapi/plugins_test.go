package adminapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

const testTenantID2 = "test-tenant-2"

// pluginRemoteSpec returns the dynamic entity spec for admin_plugin_remote.
// The host application must register this spec before using the plugin CRUD
// endpoints. Tests register it here.
func pluginRemoteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "admin_plugin_remote",
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_admin_plugin_remote",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "url", Type: registry.ColText, NotNull: true},
				{Name: "scope", Type: registry.ColText, NotNull: true},
				{Name: "module", Type: registry.ColText, NotNull: true},
			},
		},
	}
}

// buildPluginWorld builds a registry with the admin_plugin_remote spec
// (and optionally the widget spec for hybrid tests) and returns a World.
func buildPluginWorld(t *testing.T) *fabriqtest.World {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(pluginRemoteSpec()); err != nil {
		t.Fatalf("register admin_plugin_remote: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return fabriqtest.NewWorld(reg)
}

// postPlugin issues POST /admin/plugins with the given body JSON.
func postPlugin(t *testing.T, srv *httptest.Server, body map[string]any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/plugins", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(testTenantHeader, testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// postPluginForTenant issues POST /admin/plugins for a specific tenant.
func postPluginForTenant(t *testing.T, srv *httptest.Server, tenantID string, body map[string]any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/plugins", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(testTenantHeader, tenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// getPluginsForTenant issues GET /admin/plugins for a specific tenant.
func getPluginsForTenant(t *testing.T, srv *httptest.Server, tenantID string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/admin/plugins", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, tenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// deletePlugin issues DELETE /admin/plugins/{id}.
func deletePlugin(t *testing.T, srv *httptest.Server, id string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/admin/plugins/"+id, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// decodePluginResponse decodes a pluginRemote from an HTTP response body.
func decodePluginResponse(t *testing.T, resp *http.Response) pluginRemote {
	t.Helper()
	defer resp.Body.Close()
	var got pluginRemote
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return got
}

// decodePluginListResponse decodes a pluginListResponse from an HTTP response body.
func decodePluginListResponse(t *testing.T, resp *http.Response) pluginListResponse {
	t.Helper()
	defer resp.Body.Close()
	var got pluginListResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return got
}

// TestAdminPlugins_Create verifies POST /admin/plugins returns 201 with a
// generated id and echoes name, url, scope, module.
func TestAdminPlugins_Create(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postPlugin(t, srv, map[string]any{
		"name":   "entity-browser",
		"url":    "http://cdn.example.com/entity-browser.js",
		"scope":  "admin",
		"module": "./EntityBrowser",
	})
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	got := decodePluginResponse(t, resp)
	if got.ID == "" {
		t.Error("id must not be empty (server-generated)")
	}
	if got.Name != "entity-browser" {
		t.Errorf("name = %q, want %q", got.Name, "entity-browser")
	}
	if got.URL != "http://cdn.example.com/entity-browser.js" {
		t.Errorf("url = %q, want %q", got.URL, "http://cdn.example.com/entity-browser.js")
	}
	if got.Scope != "admin" {
		t.Errorf("scope = %q, want %q", got.Scope, "admin")
	}
	if got.Module != "./EntityBrowser" {
		t.Errorf("module = %q, want %q", got.Module, "./EntityBrowser")
	}
}

// TestAdminPlugins_List verifies GET /admin/plugins returns the posted spec.
func TestAdminPlugins_List(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// POST one plugin.
	postResp := postPlugin(t, srv, map[string]any{
		"name":   "entity-browser",
		"url":    "http://cdn.example.com/entity-browser.js",
		"scope":  "admin",
		"module": "./EntityBrowser",
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	created := decodePluginResponse(t, postResp)

	// GET all plugins.
	listResp := get(t, srv, "/admin/plugins")
	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		listResp.Body.Close()
		t.Fatalf("list status = %d, body = %s", listResp.StatusCode, body)
	}
	list := decodePluginListResponse(t, listResp)

	if len(list.Items) != 1 {
		t.Fatalf("items len = %d, want 1", len(list.Items))
	}
	if list.Items[0].ID != created.ID {
		t.Errorf("item id = %q, want %q", list.Items[0].ID, created.ID)
	}
	if list.Items[0].URL != "http://cdn.example.com/entity-browser.js" {
		t.Errorf("item url = %q, want expected url", list.Items[0].URL)
	}
}

// TestAdminPlugins_List_Two verifies that two POSTed plugins both appear in GET.
func TestAdminPlugins_List_Two(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	for _, body := range []map[string]any{
		{"name": "plugin-a", "url": "http://cdn.example.com/a.js", "scope": "admin", "module": "./A"},
		{"name": "plugin-b", "url": "http://cdn.example.com/b.js", "scope": "ops", "module": "./B"},
	} {
		resp := postPlugin(t, srv, body)
		if resp.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("post status = %d, body = %s", resp.StatusCode, b)
		}
		resp.Body.Close()
	}

	listResp := get(t, srv, "/admin/plugins")
	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		listResp.Body.Close()
		t.Fatalf("list status = %d, body = %s", listResp.StatusCode, body)
	}
	list := decodePluginListResponse(t, listResp)
	if len(list.Items) != 2 {
		t.Fatalf("items len = %d, want 2", len(list.Items))
	}
}

// TestAdminPlugins_Delete verifies DELETE /admin/plugins/{id} returns 204 and
// the subsequent GET no longer includes the deleted item.
func TestAdminPlugins_Delete(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// POST.
	postResp := postPlugin(t, srv, map[string]any{
		"name":   "to-delete",
		"url":    "http://cdn.example.com/del.js",
		"scope":  "admin",
		"module": "./Del",
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	created := decodePluginResponse(t, postResp)

	// DELETE.
	delResp := deletePlugin(t, srv, created.ID)
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(delResp.Body)
		t.Fatalf("delete status = %d, body = %s", delResp.StatusCode, body)
	}

	// GET should show empty list.
	listResp := get(t, srv, "/admin/plugins")
	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		listResp.Body.Close()
		t.Fatalf("list status = %d, body = %s", listResp.StatusCode, body)
	}
	list := decodePluginListResponse(t, listResp)
	if len(list.Items) != 0 {
		t.Fatalf("items len = %d, want 0 after delete", len(list.Items))
	}
}

// TestAdminPlugins_Delete_NotFound verifies DELETE of an unknown id returns 404.
func TestAdminPlugins_Delete_NotFound(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := deletePlugin(t, srv, "no-such-id")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// TestAdminPlugins_Create_MissingURL verifies POST without url returns 400.
func TestAdminPlugins_Create_MissingURL(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postPlugin(t, srv, map[string]any{
		"name":   "no-url",
		"scope":  "admin",
		"module": "./NoURL",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// TestAdminPlugins_Create_MissingScope verifies POST without scope returns 400.
func TestAdminPlugins_Create_MissingScope(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postPlugin(t, srv, map[string]any{
		"name":   "no-scope",
		"url":    "http://cdn.example.com/noscope.js",
		"module": "./NoScope",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// TestAdminPlugins_Create_MissingModule verifies POST without module returns 400.
func TestAdminPlugins_Create_MissingModule(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postPlugin(t, srv, map[string]any{
		"name":  "no-module",
		"url":   "http://cdn.example.com/nomod.js",
		"scope": "admin",
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// TestAdminPlugins_Create_ExecFailure_RendersStructuredError verifies that a
// write failure on fab.Exec (simulated here as an outbox-append failure deep
// in the fake store's transaction, standing in for a driver-level failure in
// the real postgres adapter) now goes through renderError instead of the old
// forge.InternalError. The response must be the structured errorBody shape
// (status 500, error.code = "internal") and must NOT leak the raw injected
// error text (which deliberately looks like a driver error) anywhere in the
// top-level body — proving the leak vector this fix wave closes.
func TestAdminPlugins_Create_ExecFailure_RendersStructuredError(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	const driverLeakText = "pq: duplicate key value violates unique constraint SQLSTATE 23505 pgdriver"
	world.Store.FailOnOutbox(func() error {
		return errors.New(driverLeakText)
	})
	defer world.Store.FailOnOutbox(nil)

	resp := postPlugin(t, srv, map[string]any{
		"name":   "leaky-plugin",
		"url":    "http://cdn.example.com/leaky.js",
		"scope":  "admin",
		"module": "./Leaky",
	})
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %s", resp.StatusCode, body)
	}

	if strings.Contains(string(body), "pgdriver") || strings.Contains(string(body), "SQLSTATE") {
		t.Fatalf("response body leaked raw driver text: %s", body)
	}

	var got errorBody
	if decErr := json.Unmarshal(body, &got); decErr != nil {
		t.Fatalf("decode errorBody: %v; body = %s", decErr, body)
	}
	if got.Error.Code != "internal" {
		t.Errorf("error.code = %q, want %q", got.Error.Code, "internal")
	}
	if got.Error.Message == "" || strings.Contains(got.Error.Message, driverLeakText) {
		t.Errorf("error.message = %q, must be the generic safe message, not the driver text", got.Error.Message)
	}
}

// TestAdminPlugins_TenantIsolation verifies that a plugin written under tenant A
// is not visible to tenant B.
func TestAdminPlugins_TenantIsolation(t *testing.T) {
	world := buildPluginWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// POST under tenant A (testTenantID).
	postResp := postPlugin(t, srv, map[string]any{
		"name":   "tenant-a-plugin",
		"url":    "http://cdn.example.com/a.js",
		"scope":  "admin",
		"module": "./A",
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	postResp.Body.Close()

	// Ensure tenant B exists in the tenant context (seed a valid tenant entry).
	// We POST for tenant B to seed the tenant store, then list for B.
	postB := postPluginForTenant(t, srv, testTenantID2, map[string]any{
		"name":   "tenant-b-plugin",
		"url":    "http://cdn.example.com/b.js",
		"scope":  "ops",
		"module": "./B",
	})
	if postB.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postB.Body)
		postB.Body.Close()
		t.Fatalf("post-b status = %d, body = %s", postB.StatusCode, body)
	}
	postB.Body.Close()

	// GET for tenant B — must only see tenant B's plugin.
	listB := getPluginsForTenant(t, srv, testTenantID2)
	if listB.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listB.Body)
		listB.Body.Close()
		t.Fatalf("list-b status = %d, body = %s", listB.StatusCode, body)
	}
	gotB := decodePluginListResponse(t, listB)
	if len(gotB.Items) != 1 {
		t.Fatalf("tenant B items len = %d, want 1", len(gotB.Items))
	}
	if gotB.Items[0].Name != "tenant-b-plugin" {
		t.Errorf("tenant B item name = %q, want %q", gotB.Items[0].Name, "tenant-b-plugin")
	}

	// GET for tenant A — must only see tenant A's plugin.
	listA := getPluginsForTenant(t, srv, testTenantID)
	if listA.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listA.Body)
		listA.Body.Close()
		t.Fatalf("list-a status = %d, body = %s", listA.StatusCode, body)
	}
	gotA := decodePluginListResponse(t, listA)
	if len(gotA.Items) != 1 {
		t.Fatalf("tenant A items len = %d, want 1", len(gotA.Items))
	}
	if gotA.Items[0].Name != "tenant-a-plugin" {
		t.Errorf("tenant A item name = %q, want %q", gotA.Items[0].Name, "tenant-a-plugin")
	}
}
