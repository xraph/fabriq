package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

// digestNodeSpec returns the typed digest_node entity spec (mirrors
// domain.Register's registration of the distillation Merkle-tree node). Its
// presence is the registry-derived "distillation plane configured" signal.
func digestNodeSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name:      agent.DigestEntity,
		Kind:      registry.KindAggregate,
		Model:     (*domain.DigestNode)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	}
}

// buildDistillWorld registers the typed digest_node entity (so the distillation
// plane reads as configured) on top of the standard widget world, then seeds a
// tiny three-level tree under the test tenant: an L2 tenant root → one L1 scope
// node → one L0 entity leaf. Seeding goes through the command executor with a
// typed *domain.DigestNode payload, exactly as the core Distiller does.
func buildDistillWorld(t *testing.T) *fabriqtest.World {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Register(digestNodeSpec()); err != nil {
		t.Fatalf("register digest_node: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}

	world := fabriqtest.NewWorld(reg)
	exec, err := command.NewExecutor(reg, world.Store)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}
	ctx, err := tenant.WithTenant(t.Context(), testTenantID)
	if err != nil {
		t.Fatalf("with tenant: %v", err)
	}

	scopeID := agent.ScopeID("category", "tools")
	leafID := agent.L0ID("widget", "w1")
	rows := []*domain.DigestNode{
		{
			ID: agent.TenantRootID(), Version: 1, Level: agent.LevelTenant, Kind: agent.KindTenantNode,
			ContentHash: "ch-root", SemHash: "0000000000000001",
			ChildIDs: []string{scopeID}, ParentIDs: []string{},
		},
		{
			ID: scopeID, Version: 1, Level: agent.LevelScope, Kind: agent.KindScopeNode,
			ScopeName: "category", ScopeID: "tools",
			ContentHash: "ch-scope", SemHash: "0000000000000002",
			ChildIDs: []string{leafID}, ParentIDs: []string{agent.TenantRootID()},
		},
		{
			ID: leafID, Version: 1, Level: agent.LevelEntity, Kind: agent.KindEntityNode,
			SourceKind: "widget", SourceID: "w1",
			ContentHash: "ch-leaf", SemHash: "0000000000000003",
			ChildIDs: []string{}, ParentIDs: []string{scopeID},
		},
	}
	for _, n := range rows {
		if _, execErr := exec.Exec(ctx, command.Command{
			Entity:  agent.DigestEntity,
			Op:      command.OpUpsert,
			AggID:   n.ID,
			Payload: n,
		}); execErr != nil {
			t.Fatalf("seed digest node %s: %v", n.ID, execErr)
		}
	}
	return world
}

// TestDistillMap_NotConfigured verifies that GET /admin/distill/map returns 200
// with an empty node list when NO digest_node entity is registered (no
// distillation plane) — the map of a tenant with no digest data is simply empty.
func TestDistillMap_NotConfigured(t *testing.T) {
	world := buildTestWorld(t) // widget-only: no digest plane
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/distill/map")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	var got distillMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.RootID != agent.TenantRootID() {
		t.Errorf("rootId = %q, want %q", got.RootID, agent.TenantRootID())
	}
	if len(got.Nodes) != 0 {
		t.Errorf("nodes len = %d, want 0", len(got.Nodes))
	}
}

// TestDistillMap_Tree verifies that GET /admin/distill/map returns the seeded
// L2→L1→L0 tree, deterministically sorted by Level ascending then ID.
func TestDistillMap_Tree(t *testing.T) {
	world := buildDistillWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/distill/map")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	var got distillMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Fatalf("nodes len = %d, want 3 (root + scope + leaf)", len(got.Nodes))
	}
	// Sorted by Level ascending: L0 leaf, L1 scope, L2 root.
	wantLevels := []int{agent.LevelEntity, agent.LevelScope, agent.LevelTenant}
	for i, n := range got.Nodes {
		if n.Level != wantLevels[i] {
			t.Errorf("node[%d].level = %d, want %d", i, n.Level, wantLevels[i])
		}
		if n.ContentHash == "" {
			t.Errorf("node[%d] (%s) contentHash must not be empty", i, n.ID)
		}
	}
	if got.Nodes[2].ID != agent.TenantRootID() {
		t.Errorf("top node id = %q, want %q", got.Nodes[2].ID, agent.TenantRootID())
	}
	if got.Nodes[1].Kind != agent.KindScopeNode {
		t.Errorf("scope node kind = %q, want %q", got.Nodes[1].Kind, agent.KindScopeNode)
	}
}

// TestDistillNode_NotConfigured verifies that GET /admin/distill/node/:id
// returns 501 when no digest_node entity is registered (plane not configured).
func TestDistillNode_NotConfigured(t *testing.T) {
	world := buildTestWorld(t) // widget-only: no digest plane
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/distill/node/digest:2:tenant")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
}

// TestDistillNode_Root verifies that GET /admin/distill/node/digest:2:tenant
// returns the root node with its single scope child.
func TestDistillNode_Root(t *testing.T) {
	world := buildDistillWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/distill/node/digest:2:tenant")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	var got distillNodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Node.ID != agent.TenantRootID() {
		t.Errorf("node id = %q, want %q", got.Node.ID, agent.TenantRootID())
	}
	if got.Node.Level != agent.LevelTenant {
		t.Errorf("node level = %d, want %d", got.Node.Level, agent.LevelTenant)
	}
	if len(got.Children) != 1 {
		t.Fatalf("children len = %d, want 1 (the scope node)", len(got.Children))
	}
	if got.Children[0].ID != agent.ScopeID("category", "tools") {
		t.Errorf("child id = %q, want scope node id", got.Children[0].ID)
	}
	if got.Children[0].Kind != agent.KindScopeNode {
		t.Errorf("child kind = %q, want %q", got.Children[0].Kind, agent.KindScopeNode)
	}
}

// TestDistillNode_NotFound verifies that GET /admin/distill/node/:id returns 404
// when the digest plane IS configured but the id names no node in the tree.
func TestDistillNode_NotFound(t *testing.T) {
	world := buildDistillWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/distill/node/digest:0:widget:nope")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404, body = %s", resp.StatusCode, body)
	}
}
