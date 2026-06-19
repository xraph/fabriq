package domain

import (
	"time"

	"github.com/xraph/fabriq/core/registry"
)

// RegisterAll registers the TWINOS domain pack. Call it once at startup;
// follow with reg.Validate() (fabriq.New does both).
func RegisterAll(reg *registry.Registry) error {
	specs := make([]registry.EntitySpec, 0, 5)
	specs = append(specs, []registry.EntitySpec{
		{
			Name:      "site",
			Kind:      registry.KindAggregate,
			Model:     (*Site)(nil),
			GraphNode: "Site",
			Search:    registry.SearchSpec{Index: "sites", Fields: []string{"name", "code", "region"}},
			Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		},
		{
			Name:      "asset",
			Kind:      registry.KindAggregate,
			Model:     (*Asset)(nil),
			GraphNode: "Asset",
			Edges: []registry.EdgeSpec{
				{Field: "site_id", Rel: "LOCATED_AT", Target: "site"},
				{Field: "parent_id", Rel: "CHILD_OF", Target: "asset"},
			},
			Search:    registry.SearchSpec{Index: "assets", Fields: []string{"name", "kind", "serial"}},
			Subscribe: []registry.Scope{registry.ByID, registry.ByField("site", "site_id"), registry.ByTenant},
			Live:      &registry.LiveSpec{Filterable: []string{"name", "kind", "site_id"}, Sortable: []string{"name", "kind"}, MaxWindow: 500},
		},
		{
			Name:      "tag",
			Kind:      registry.KindAggregate,
			Model:     (*Tag)(nil),
			GraphNode: "Tag",
			Edges: []registry.EdgeSpec{
				{Field: "asset_id", Rel: "MEASURES", Target: "asset"},
			},
			Search:    registry.SearchSpec{Index: "tags", Fields: []string{"name", "unit"}},
			Subscribe: []registry.Scope{registry.ByID, registry.ByField("asset", "asset_id"), registry.ByTenant},
		},
		{
			Name:  "link",
			Kind:  registry.KindAggregate,
			Model: (*Link)(nil),
			GraphEdge: &registry.GraphEdgeSpec{
				TypeField: "kind", SourceField: "source_id", TargetField: "target_id",
				SourceLabel: "Asset", TargetLabel: "Asset", PropFields: []string{"note"},
			},
			Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		},
		{
			Name:      "blob_object",
			Kind:      registry.KindAggregate,
			Model:     (*BlobObject)(nil),
			Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		},
	}...)
	specs = append(specs, registry.EntitySpec{
		Name:      "fs_node",
		Kind:      registry.KindAggregate,
		Model:     (*FsNode)(nil),
		GraphNode: "FsNode",
		Edges: []registry.EdgeSpec{
			{Field: "parent_id", Rel: "CHILD_OF", Target: "fs_node"},
		},
		Search:    registry.SearchSpec{Index: "fs_nodes", Fields: []string{"name", "content_type"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("parent", "parent_id"), registry.ByTenant},
		Live: &registry.LiveSpec{
			Filterable: []string{"parent_id", "node_type", "name", "deleted_at"},
			Sortable:   []string{"name", "size", "updated_at", "node_type"},
			MaxWindow:  500,
		},
	})
	specs = append(specs, registry.EntitySpec{
		Name:  "page",
		Kind:  registry.KindDocument,
		Model: (*Page)(nil),
		CRDT: &registry.CRDTSpec{
			Engine:        "grove-crdt",
			SnapshotEvery: 64,
			QuietWindow:   2 * time.Second,
		},
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	})
	specs = append(specs, registry.EntitySpec{
		Name:      "blob_source",
		Kind:      registry.KindAggregate,
		Model:     (*BlobSource)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	})
	specs = append(specs, registry.EntitySpec{
		Name:      "fs_permission",
		Kind:      registry.KindAggregate,
		Model:     (*FsPermission)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("node", "node_id"), registry.ByTenant},
	})
	specs = append(specs, registry.EntitySpec{
		Name:      "fs_share",
		Kind:      registry.KindAggregate,
		Model:     (*FsShare)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("node", "node_id"), registry.ByTenant},
	})
	for _, spec := range specs {
		if err := reg.Register(spec); err != nil {
			return err
		}
	}
	return nil
}
