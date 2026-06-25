package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// embeddedWidgetSpec returns a dynamic entity that opts into vector embedding,
// the search projection, the document (CRDT) plane, and the graph layer, so the
// per-type capability detector has every real flag to assert against.
func embeddedWidgetSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "gadget", Kind: registry.KindAggregate,
		GraphNode: "Gadget",
		Search:    registry.SearchSpec{Index: "gadgets", Fields: []string{"name"}},
		Embed:     &registry.EmbedSpec{Fields: []string{"name"}},
		Schema: &registry.DynamicSchema{
			Table: "ds_gadgets",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
			},
		},
	}
}

// buildCapabilityWorld registers the plain widget plus the fully-decorated
// gadget so per-type detection has both a bare and an enriched type.
func buildCapabilityWorld(t *testing.T) *fabriqtest.World {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Register(embeddedWidgetSpec()); err != nil {
		t.Fatalf("register gadget: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}
	return fabriqtest.NewWorld(reg)
}

// TestCapabilities_Instance verifies GET /admin/capabilities reports the
// instance-level subsystem map. The fake fabric wires every port (graph,
// search, vector, spatial, blob) as a non-stub fake, so all probed flags are
// true; crdt is registry-derived and the gadget type opts into the document
// plane via... no — neither widget nor gadget is KindDocument here, so crdt is
// false at instance level.
func TestCapabilities_Instance(t *testing.T) {
	world := buildCapabilityWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/capabilities")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got instanceCapabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !got.Capabilities.Relational {
		t.Error("relational must be true for any fabriq instance")
	}
	// The fake fabric wires non-stub fakes for these ports, so the probe must
	// report them configured.
	if !got.Capabilities.Graph {
		t.Error("graph must be true (fake graph querier is wired)")
	}
	if !got.Capabilities.Vector {
		t.Error("vector must be true (fake vector querier is wired)")
	}
	if !got.Capabilities.Spatial {
		t.Error("spatial must be true (fake spatial querier is wired)")
	}
	if !got.Capabilities.Search {
		t.Error("search must be true (fake search querier is wired)")
	}
	if !got.Capabilities.Files {
		t.Error("files must be true (fake blob store is wired)")
	}
}

// TestCapabilities_Instance_CRDT verifies that registering a KindDocument
// entity flips the registry-derived instance-level crdt flag to true.
func TestCapabilities_Instance_CRDT(t *testing.T) {
	reg := registry.New()
	docSpec := registry.EntitySpec{
		Name: "note", Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt"},
		Schema: &registry.DynamicSchema{
			Table:   "ds_notes",
			Columns: []registry.DynamicColumn{{Name: "body", Type: registry.ColText}},
		},
	}
	if err := reg.Register(docSpec); err != nil {
		t.Fatalf("register note: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	world := fabriqtest.NewWorld(reg)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/capabilities")
	defer resp.Body.Close()

	var got instanceCapabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Capabilities.CRDT {
		t.Error("crdt must be true when a KindDocument entity is registered")
	}
}

// TestCapabilities_PerType_Bare verifies the per-type object for a plain
// dynamic entity: relational true, every other flag false.
func TestCapabilities_PerType_Bare(t *testing.T) {
	world := buildCapabilityWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/capabilities?type=widget")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got typeCapabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != "widget" {
		t.Errorf("type = %q, want %q", got.Type, "widget")
	}
	if !got.Capabilities.Relational {
		t.Error("relational must be true for a registered dynamic type")
	}
	if got.Capabilities.Vector {
		t.Error("vector must be false for a non-embedded type")
	}
	if got.Capabilities.Search {
		t.Error("search must be false for a non-searchable type")
	}
	if got.Capabilities.Graph {
		t.Error("graph must be false for a non-graph type")
	}
	if got.Capabilities.CRDT {
		t.Error("crdt must be false for a KindAggregate type")
	}
	if got.Capabilities.Spatial {
		t.Error("spatial must be false (no geometry column kind exists)")
	}
}

// TestCapabilities_PerType_Enriched verifies that the per-type detector reads
// the real declarative EntitySpec signals: Embed → vector, Search → search,
// GraphNode → graph.
func TestCapabilities_PerType_Enriched(t *testing.T) {
	world := buildCapabilityWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/capabilities?type=gadget")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got typeCapabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Capabilities.Relational {
		t.Error("relational must be true")
	}
	if !got.Capabilities.Vector {
		t.Error("vector must be true (Embed spec is set)")
	}
	if !got.Capabilities.Search {
		t.Error("search must be true (Search.Index is set)")
	}
	if !got.Capabilities.Graph {
		t.Error("graph must be true (GraphNode is set)")
	}
}

// TestCapabilities_PerType_Unknown verifies an unregistered type yields 400.
func TestCapabilities_PerType_Unknown(t *testing.T) {
	world := buildCapabilityWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/capabilities?type=does-not-exist")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, body)
	}
}
