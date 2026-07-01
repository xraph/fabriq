package adminapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// instanceCapabilities reports which fabric subsystems this fabriq instance has
// configured. Each flag answers "can the instance serve this subsystem at all",
// independent of any particular entity type.
//
// Detection method per flag:
//
//	relational  ALWAYS true — the relational querier is mandatory (fabriq.New
//	            rejects a nil relational port), so a running instance always
//	            has it. Not probed.
//	graph       REAL — probed: an unconfigured graph port is the package-private
//	            notConfigured stub, which answers every read with
//	            fabriqerr.ErrStoreNotConfigured. A configured adapter (or test
//	            fake) returns some other error (or nil), so "not
//	            ErrStoreNotConfigured" == configured.
//	vector      REAL — probed the same way via VectorQuerier.Get.
//	spatial     REAL — probed the same way via SpatialQuerier.Within.
//	search      REAL — probed the same way via SearchQuerier.Search.
//	files       REAL — probed the same way via blob.Store.Head.
//	crdt        REGISTRY-DERIVED (not probed). The document plane is deferred
//	            framework-wide: even a wired document store returns
//	            ErrStoreNotConfigured today, so a port probe cannot distinguish
//	            "absent" from "deferred fake". Instead crdt reports whether ANY
//	            registered entity opts into the document plane (Kind ==
//	            KindDocument or a CRDTSpec is declared) — a deterministic,
//	            side-effect-free signal. This becomes a true port probe once the
//	            document plane ships a non-deferred adapter.
//	distill     REGISTRY-DERIVED (not probed). The context-distillation tree
//	            lives in the digest_node entity; the plane is "present" exactly
//	            when that entity is registered. Like crdt, this is a
//	            deterministic, side-effect-free registry signal (a Toolkit read
//	            against an unregistered digest_node is a no-op), so no port probe
//	            is needed.
type instanceCapabilities struct {
	Relational bool `json:"relational"`
	Graph      bool `json:"graph"`
	Vector     bool `json:"vector"`
	Spatial    bool `json:"spatial"`
	Search     bool `json:"search"`
	CRDT       bool `json:"crdt"`
	Files      bool `json:"files"`
	Distill    bool `json:"distill"`
	// Timeseries is REAL — probed via TSQuerier.Range: an unconfigured stub
	// answers ErrStoreNotConfigured, a real adapter rejects the empty-series
	// probe with a validation error first. See timeseriesConfigured.
	Timeseries bool `json:"timeseries"`
}

// instanceCapabilitiesResponse is the payload for GET {BasePath}/capabilities
// (no ?type=).
type instanceCapabilitiesResponse struct {
	Capabilities instanceCapabilities `json:"capabilities"`
}

// typeCapabilities reports which subsystems a single dynamic entity type
// participates in, read from its declarative registry EntitySpec.
//
// Detection method per flag:
//
//	relational  ALWAYS true — every registered dynamic type is relational-backed
//	            (postgres is the source of truth).
//	vector      REAL — EntitySpec.Embed != nil (the entity opts into vector
//	            embedding / auto-indexing).
//	search      REAL — EntitySpec.Search.Index != "" (the entity is mapped into
//	            the search projection).
//	graph       REAL — EntitySpec.GraphNode != "" OR a GraphEdge spec OR one or
//	            more Edges are declared (the entity projects into the graph as a
//	            node, a reified edge, or via FK→relationship mappings).
//	crdt        REAL — EntitySpec.Kind == KindDocument OR a CRDTSpec is declared
//	            (the entity lives in the collaborative document plane).
//	spatial     HARDCODED false — NOT detectable. The registry's neutral column
//	            type set (registry.ColumnType: Text/Int/Float/Bool/Time/JSON) has
//	            NO geometry/spatial kind, so a dynamic schema cannot declare a
//	            geometry column and there is nothing on the EntitySpec to mark
//	            spatial participation. Detecting this would require either a new
//	            geometry ColumnType in core/registry or a SpatialSpec field on
//	            EntitySpec; until one exists this flag is always false.
type typeCapabilities struct {
	Relational bool `json:"relational"`
	Vector     bool `json:"vector"`
	Search     bool `json:"search"`
	Spatial    bool `json:"spatial"`
	CRDT       bool `json:"crdt"`
	Graph      bool `json:"graph"`
}

// typeCapabilitiesResponse is the payload for GET {BasePath}/capabilities?type=.
type typeCapabilitiesResponse struct {
	Type         string           `json:"type"`
	Capabilities typeCapabilities `json:"capabilities"`
}

// registerCapabilityRoutes wires the capability-introspection route onto the
// given router. It shares the same route options (auth/tenant middleware) as
// the rest of the admin surface. Capabilities are structural, so the data is
// tenant-agnostic, but the route stays behind the host's middleware for a
// uniform security boundary.
func (c *adminController) registerCapabilityRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	capOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.capabilities"),
		forge.WithSummary("Report fabric subsystem capabilities (instance-level, or per-type with ?type=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/capabilities", c.handleCapabilities, capOpts...)
}

// handleCapabilities serves GET {BasePath}/capabilities.
//
// With no query params it returns the instance-level subsystem map. With
// ?type=<entityName> it returns the per-type participation map for that dynamic
// entity; an unknown type yields 400.
func (c *adminController) handleCapabilities(ctx forge.Context) error {
	if entityType := ctx.Query("type"); entityType != "" {
		return c.handleTypeCapabilities(ctx, entityType)
	}
	return c.handleInstanceCapabilities(ctx)
}

// handleInstanceCapabilities serves the instance-level subsystem map. It probes
// the optional capability ports for the not-configured sentinel (see
// instanceCapabilities for the per-flag detection method) and derives the crdt
// flag from the registry.
func (c *adminController) handleInstanceCapabilities(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	// Probe with a fresh, tenant-less context: the notConfigured stubs answer
	// ErrStoreNotConfigured BEFORE any tenant check, while real adapters and
	// test fakes require a tenant and so return a different (non-sentinel)
	// error first. That difference is exactly the configured/unconfigured
	// signal, and the tenant-less context guarantees the probe performs no real
	// work and has no side effects.
	probeCtx := context.Background()

	caps := instanceCapabilities{
		Relational: true, // mandatory port; always present on a running instance.
		Graph:      graphConfigured(probeCtx, fab.Graph()),
		Vector:     vectorConfigured(probeCtx, fab.Vector()),
		Spatial:    spatialConfigured(probeCtx, fab.Spatial()),
		Search:     searchConfigured(probeCtx, fab.Search()),
		Files:      blobConfigured(probeCtx, fab.Blob()),
		CRDT:       registryHasDocumentPlane(reg),
		Distill:    registryHasDistillPlane(reg),
		Timeseries: timeseriesConfigured(probeCtx, fab.Timeseries()),
	}

	return ctx.JSON(http.StatusOK, instanceCapabilitiesResponse{Capabilities: caps})
}

// handleTypeCapabilities serves the per-type participation map for entityType,
// read from its declarative EntitySpec. An unknown or non-dynamic type yields
// 400.
func (c *adminController) handleTypeCapabilities(ctx forge.Context, entityType string) error {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	ent, ok := reg.Get(entityType)
	if !ok || ent.Spec.Schema == nil {
		return forge.BadRequest("unknown dynamic entity type: " + entityType)
	}

	spec := ent.Spec
	caps := typeCapabilities{
		Relational: true, // every registered dynamic type is relational-backed.
		Vector:     spec.Embed != nil,
		Search:     spec.Search.Index != "",
		Graph:      spec.GraphNode != "" || spec.GraphEdge != nil || len(spec.Edges) > 0,
		CRDT:       spec.Kind == registry.KindDocument || spec.CRDT != nil,
		Spatial:    false, // not detectable; see typeCapabilities docs.
	}

	return ctx.JSON(http.StatusOK, typeCapabilitiesResponse{Type: entityType, Capabilities: caps})
}

// notConfigured reports whether err is (or wraps) the store-not-configured
// sentinel returned by an unconfigured capability port's stub.
func notConfigured(err error) bool {
	return errors.Is(err, fabriqerr.ErrStoreNotConfigured)
}

// graphConfigured probes the graph port: an unconfigured stub answers with
// ErrStoreNotConfigured; anything else means a real backend is wired.
func graphConfigured(ctx context.Context, g query.GraphQuerier) bool {
	var ids []string
	return !notConfigured(g.Query(ctx, "", nil, &ids))
}

// vectorConfigured probes the vector port via Get (read-only, no side effects).
func vectorConfigured(ctx context.Context, v query.VectorQuerier) bool {
	_, err := v.Get(ctx, "", "")
	return !notConfigured(err)
}

// spatialConfigured probes the spatial port via Within (read-only).
func spatialConfigured(ctx context.Context, s query.SpatialQuerier) bool {
	var matches []query.SpatialMatch
	return !notConfigured(s.Within(ctx, query.SpatialQuery{}, &matches))
}

// searchConfigured probes the search port via Search (read-only).
func searchConfigured(ctx context.Context, s query.SearchQuerier) bool {
	var rows []map[string]any
	return !notConfigured(s.Search(ctx, query.SearchQuery{}, &rows))
}

// blobConfigured probes the blob (files) port via Head (read-only).
func blobConfigured(ctx context.Context, b blob.Store) bool {
	_, err := b.Head(ctx, "")
	return !notConfigured(err)
}

// registryHasDocumentPlane reports whether any registered entity opts into the
// document (CRDT) plane. Used for the instance-level crdt flag because the
// document plane is deferred and cannot be probed (see instanceCapabilities).
func registryHasDocumentPlane(reg *registry.Registry) bool {
	for _, ent := range reg.All() {
		if ent.Spec.Kind == registry.KindDocument || ent.Spec.CRDT != nil {
			return true
		}
	}
	return false
}

// registryHasDistillPlane reports whether the context-distillation plane is
// configured. The distillation Merkle tree is stored in the digest_node entity,
// so the plane is present exactly when that entity is registered. Used for the
// instance-level distill flag (no port probe — the digest tree is read through
// the relational plane, which is always present).
func registryHasDistillPlane(reg *registry.Registry) bool {
	_, ok := reg.Get(agent.DigestEntity)
	return ok
}
