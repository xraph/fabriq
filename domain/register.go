package domain

import (
	"time"

	"github.com/xraph/fabriq/core/registry"
)

// RegisterAll registers the TWINOS domain pack. Call it once at startup;
// follow with reg.Validate() (fabriq.New does both).
func RegisterAll(reg *registry.Registry) error {
	specs := make([]registry.EntitySpec, 0, 4)
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
	}...)
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
	for _, spec := range specs {
		if err := reg.Register(spec); err != nil {
			return err
		}
	}
	return nil
}
