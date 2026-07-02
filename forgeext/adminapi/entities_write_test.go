package adminapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// doWrite issues a request with a JSON body and the test tenant header.
func doWrite(t *testing.T, method, url string, body map[string]any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(b))
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

// postEntity issues POST {base}/entities for the test tenant.
func postEntity(t *testing.T, srv *httptest.Server, body map[string]any) *http.Response {
	t.Helper()
	return doWrite(t, http.MethodPost, srv.URL+"/admin/entities", body)
}

// putEntity issues PUT {base}/entities/{id} for the test tenant.
func putEntity(t *testing.T, srv *httptest.Server, id string, body map[string]any) *http.Response {
	t.Helper()
	return doWrite(t, http.MethodPut, srv.URL+"/admin/entities/"+id, body)
}

// deleteEntity issues DELETE {base}/entities/{id}?type=... for the test tenant.
func deleteEntity(t *testing.T, srv *httptest.Server, id, entityType string) *http.Response {
	t.Helper()
	url := srv.URL + "/admin/entities/" + id
	if entityType != "" {
		url += "?type=" + entityType
	}
	req, err := http.NewRequest(http.MethodDelete, url, nil)
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

// decodeEntityItem decodes an entityItem from an HTTP response body.
func decodeEntityItem(t *testing.T, resp *http.Response) entityItem {
	t.Helper()
	defer resp.Body.Close()
	var got entityItem
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode entity item: %v", err)
	}
	return got
}

// TestAdminEntities_Create verifies POST /admin/entities returns 201 with a
// generated id and echoes the data; a subsequent GET returns the row.
func TestAdminEntities_Create(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postEntity(t, srv, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Gizmo", "colour": "green"},
	})
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	created := decodeEntityItem(t, resp)
	if created.ID == "" {
		t.Fatal("id must not be empty (server-generated)")
	}
	if created.Type != "widget" {
		t.Errorf("type = %q, want widget", created.Type)
	}
	if created.Data["name"] != "Gizmo" {
		t.Errorf("data.name = %v, want Gizmo", created.Data["name"])
	}

	// GET the created row.
	getResp := get(t, srv, "/admin/entities/"+created.ID+"?type=widget")
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		t.Fatalf("get status = %d, body = %s", getResp.StatusCode, body)
	}
	got := decodeEntityItem(t, getResp)
	if got.ID != created.ID {
		t.Errorf("get id = %q, want %q", got.ID, created.ID)
	}
	if got.Data["name"] != "Gizmo" {
		t.Errorf("get data.name = %v, want Gizmo", got.Data["name"])
	}
}

// TestAdminEntities_Update verifies PUT /admin/entities/{id} updates a field and
// the change is reflected on a subsequent GET.
func TestAdminEntities_Update(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	postResp := postEntity(t, srv, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Gadget", "colour": "blue"},
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	created := decodeEntityItem(t, postResp)

	putResp := putEntity(t, srv, created.ID, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Gadget", "colour": "purple"},
	})
	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		putResp.Body.Close()
		t.Fatalf("put status = %d, body = %s", putResp.StatusCode, body)
	}
	putResp.Body.Close()

	getResp := get(t, srv, "/admin/entities/"+created.ID+"?type=widget")
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		t.Fatalf("get status = %d, body = %s", getResp.StatusCode, body)
	}
	got := decodeEntityItem(t, getResp)
	if got.Data["colour"] != "purple" {
		t.Errorf("data.colour = %v, want purple after update", got.Data["colour"])
	}
}

// TestAdminEntities_Delete verifies DELETE /admin/entities/{id}?type= returns
// 204 and the subsequent GET returns 404.
func TestAdminEntities_Delete(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	postResp := postEntity(t, srv, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Doomed", "colour": "grey"},
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	created := decodeEntityItem(t, postResp)

	delResp := deleteEntity(t, srv, created.ID, "widget")
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", delResp.StatusCode)
	}

	getResp := get(t, srv, "/admin/entities/"+created.ID+"?type=widget")
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("get-after-delete status = %d, want 404", getResp.StatusCode)
	}
}

// TestAdminEntities_Create_MissingType verifies POST without type returns 400.
func TestAdminEntities_Create_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postEntity(t, srv, map[string]any{
		"data": map[string]any{"name": "NoType"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminEntities_Create_UnknownType verifies POST with an unknown type
// returns 400.
func TestAdminEntities_Create_UnknownType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postEntity(t, srv, map[string]any{
		"type": "no-such-entity",
		"data": map[string]any{"name": "X"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminEntities_Update_MissingType verifies PUT without type returns 400.
func TestAdminEntities_Update_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := putEntity(t, srv, "some-id", map[string]any{
		"data": map[string]any{"name": "X"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminEntities_Update_NotFound verifies PUT of an unknown id returns 404.
func TestAdminEntities_Update_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := putEntity(t, srv, "01HMISSING0000000000000000", map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "Ghost", "colour": "none"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestAdminEntities_Delete_MissingType verifies DELETE without ?type= returns 400.
func TestAdminEntities_Delete_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := deleteEntity(t, srv, "some-id", "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminEntities_Delete_NotFound verifies DELETE of an unknown id returns 404.
func TestAdminEntities_Delete_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := deleteEntity(t, srv, "no-such-id", "widget")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestAdminEntities_TenantIsolation verifies a row created under tenant A is not
// deletable as tenant B (B sees no such row → 404).
func TestAdminEntities_TenantIsolation(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// Create under tenant A (testTenantID).
	postResp := postEntity(t, srv, map[string]any{
		"type": "widget",
		"data": map[string]any{"name": "TenantA", "colour": "red"},
	})
	if postResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(postResp.Body)
		postResp.Body.Close()
		t.Fatalf("post status = %d, body = %s", postResp.StatusCode, body)
	}
	created := decodeEntityItem(t, postResp)

	// Delete as tenant B — must 404 (row not visible to B).
	url := srv.URL + "/admin/entities/" + created.ID + "?type=widget"
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID2)
	delB, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	delB.Body.Close()
	if delB.StatusCode != http.StatusNotFound {
		t.Fatalf("tenant-B delete status = %d, want 404", delB.StatusCode)
	}

	// Tenant A can still delete it.
	delA := deleteEntity(t, srv, created.ID, "widget")
	delA.Body.Close()
	if delA.StatusCode != http.StatusNoContent {
		t.Fatalf("tenant-A delete status = %d, want 204", delA.StatusCode)
	}
}
