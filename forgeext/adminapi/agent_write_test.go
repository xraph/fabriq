package adminapi

import (
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
)

func TestRemember_AllowedCreate(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithWritePolicy(agent.WritePolicy{
		Allow: map[string][]command.Op{"widget": {command.OpCreate}},
	}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/agent/remember", testTenantID,
		map[string]any{"entity": "widget", "op": "create", "payload": map[string]any{"name": "Remembered"}})
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
	if out.Result.AggID == "" || out.Result.Version != 1 {
		t.Fatalf("unexpected result: %+v", out.Result)
	}
}

func TestRemember_DeniedByDefault(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // no WritePolicy → deny-all
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/agent/remember", testTenantID,
		map[string]any{"entity": "widget", "op": "create", "payload": map[string]any{"name": "X"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestRemember_NotAllowedOp(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithWritePolicy(agent.WritePolicy{
		Allow: map[string][]command.Op{"widget": {command.OpCreate}}, // create only
	}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/agent/remember", testTenantID,
		map[string]any{"entity": "widget", "op": "delete", "aggId": "some-id"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestWritePolicy_ReportsAllow(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithWritePolicy(agent.WritePolicy{
		Allow: map[string][]command.Op{"widget": {command.OpCreate, command.OpUpdate}},
	}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/agent/write-policy")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Allow map[string][]string `json:"allow"`
	}
	decode(t, resp, &out)
	ops := out.Allow["widget"]
	if len(ops) != 2 || ops[0] != "create" || ops[1] != "update" {
		t.Fatalf("allow[widget] = %v, want [create update]", ops)
	}
}
