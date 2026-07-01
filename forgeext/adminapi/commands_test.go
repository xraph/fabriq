package adminapi

import (
	"net/http"
	"testing"
)

// Unlike the raw-SQL / pgvector endpoints, the command plane runs on the fake
// world (command executor over FakeRelational), so these exercise the real
// happy paths as well as validation and error mapping.

func TestExecCommand_Create(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/commands", testTenantID,
		map[string]any{"entity": "widget", "op": "create", "payload": map[string]any{"name": "Gizmo"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Result struct {
			AggID   string `json:"aggId"`
			Version int64  `json:"version"`
		} `json:"result"`
	}
	decode(t, resp, &out)
	if out.Result.AggID == "" {
		t.Fatal("expected a minted aggId")
	}
	if out.Result.Version != 1 {
		t.Fatalf("version = %d, want 1", out.Result.Version)
	}
}

func TestExecBatch_AllOrNothing(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/commands/batch", testTenantID,
		map[string]any{"commands": []any{
			map[string]any{"entity": "widget", "op": "create", "payload": map[string]any{"name": "A"}},
			map[string]any{"entity": "widget", "op": "create", "payload": map[string]any{"name": "B"}},
		}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Results []struct {
			AggID string `json:"aggId"`
		} `json:"results"`
	}
	decode(t, resp, &out)
	if len(out.Results) != 2 {
		t.Fatalf("results = %d, want 2", len(out.Results))
	}
}

func TestExecCommand_UnknownOp(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/commands", testTenantID,
		map[string]any{"entity": "widget", "op": "frobnicate", "payload": map[string]any{"name": "x"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestExecCommand_UpdateMissingAggId(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/commands", testTenantID,
		map[string]any{"entity": "widget", "op": "update", "payload": map[string]any{"name": "x"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestExecBatch_Empty(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/commands/batch", testTenantID,
		map[string]any{"commands": []any{}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestExecCommand_VersionConflict(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	body := map[string]any{"entity": "widget", "op": "create", "aggId": "01HWIDGETFIXEDID0000000001",
		"payload": map[string]any{"name": "First"}}
	r1 := doWrite(t, http.MethodPost, srv.URL+"/admin/commands", testTenantID, body)
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first create status = %d, want 200", r1.StatusCode)
	}

	// Create-on-existing is a version conflict (expected version 0, stored 1).
	r2 := doWrite(t, http.MethodPost, srv.URL+"/admin/commands", testTenantID, body)
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusConflict {
		t.Fatalf("second create status = %d, want 409", r2.StatusCode)
	}
}
