package domain_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

func TestRegisterAll_ValidatesCleanly(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestRegisterAll_SiteSpec(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	site, ok := reg.Get("site")
	if !ok {
		t.Fatal("site not registered")
	}
	if site.Binding.Table != "sites" || site.Spec.GraphNode != "Site" {
		t.Fatalf("site = table %q node %q", site.Binding.Table, site.Spec.GraphNode)
	}
	if site.Spec.Search.Index != "sites" {
		t.Fatalf("site search index = %q", site.Spec.Search.Index)
	}
}

func TestRegisterAll_AssetEdges(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	asset, ok := reg.Get("asset")
	if !ok {
		t.Fatal("asset not registered")
	}
	if len(asset.Spec.Edges) != 2 {
		t.Fatalf("asset edges = %+v, want LOCATED_AT + CHILD_OF", asset.Spec.Edges)
	}
	rels := map[string]registry.EdgeSpec{}
	for _, e := range asset.Spec.Edges {
		rels[e.Rel] = e
	}
	if e := rels["LOCATED_AT"]; e.Field != "site_id" || e.Target != "site" {
		t.Fatalf("LOCATED_AT = %+v", e)
	}
	if e := rels["CHILD_OF"]; e.Field != "parent_id" || e.Target != "asset" {
		t.Fatalf("CHILD_OF = %+v", e)
	}
}

func TestRegisterAll_TagIsTelemetryMetadataOnly(t *testing.T) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	tag, ok := reg.Get("tag")
	if !ok {
		t.Fatal("tag not registered")
	}
	// The tag aggregate is metadata; its readings live in the Timescale
	// hypertable written through the event-bypass bulk path.
	if !tag.Binding.HasColumn("asset_id") || !tag.Binding.HasColumn("unit") {
		t.Fatalf("tag columns = %v", tag.Binding.Columns)
	}
	if tag.Spec.Kind != registry.KindAggregate {
		t.Fatal("tag must be an aggregate")
	}
}

func TestReadingsSeries_NameIsStable(t *testing.T) {
	// The hypertable series name is part of the public contract between
	// the bulk-ingest command and the Timescale adapter.
	if domain.ReadingsSeries != "tag_readings" {
		t.Fatalf("ReadingsSeries = %q", domain.ReadingsSeries)
	}
}
