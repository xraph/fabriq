package fabriq_test

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

func fsReg(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestFsNodeGraphProjection(t *testing.T) {
	reg := fsReg(t)
	payload, _ := json.Marshal(map[string]any{
		"id": "n1", "tenant_id": "acme", "parent_id": "p1", "name": "x", "node_type": "folder",
	})
	env := event.Envelope{
		AggID: "n1", TenantID: "acme", Aggregate: "fs_node",
		Type: "fs_node.created", Version: 1, Payload: payload,
	}
	muts, err := projection.GraphApplier(reg).Apply(env)
	if err != nil {
		t.Fatalf("GraphApplier.Apply: %v", err)
	}

	var hasNode, hasEdge bool
	for _, m := range muts {
		switch v := m.(type) {
		case projection.NodeUpsert:
			if v.Label == "FsNode" && v.ID == "n1" {
				hasNode = true
			}
		case projection.EdgeUpsert:
			if v.Rel == "CHILD_OF" {
				hasEdge = true
			}
		}
	}
	if !hasNode || !hasEdge {
		t.Fatalf("expected FsNode node + CHILD_OF edge, got node=%v edge=%v (%d muts)", hasNode, hasEdge, len(muts))
	}
}

func TestFsNodeSearchProjectionExcludesPath(t *testing.T) {
	reg := fsReg(t)
	payload, _ := json.Marshal(map[string]any{
		"id": "n1", "tenant_id": "acme", "name": "report", "content_type": "application/pdf",
		"path": "/a/b/report",
	})
	env := event.Envelope{
		AggID: "n1", TenantID: "acme", Aggregate: "fs_node",
		Type: "fs_node.created", Version: 1, Payload: payload,
	}
	muts, err := projection.SearchApplier(reg).Apply(env)
	if err != nil {
		t.Fatalf("SearchApplier.Apply: %v", err)
	}

	for _, m := range muts {
		if di, ok := m.(projection.DocIndex); ok && di.Index == "fs_nodes" {
			if _, has := di.Doc["path"]; has {
				t.Fatal("search doc must NOT include path")
			}
			if di.Doc["name"] != "report" {
				t.Fatalf("name not indexed: %+v", di.Doc)
			}
			return
		}
	}
	t.Fatal("no DocIndex for fs_nodes produced")
}
