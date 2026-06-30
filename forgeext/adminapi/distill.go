package adminapi

import (
	"net/http"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/agent"
)

// distillNode is one digest-tree node in the GET {base}/distill/map listing. It
// is a stable camelCase projection of agent.MapLine, the bird's-eye outline line
// the Toolkit returns. Summary is populated only when a CAS is configured AND
// the per-line summary text was carried by the Toolkit (Map does not retrieve
// summaries today, so it is typically empty here — drill into a node for text).
type distillNode struct {
	// ID is the digest node id (e.g. "digest:2:tenant", "digest:1:scope:...",
	// "digest:0:<kind>:<id>").
	ID string `json:"id"`
	// Level is the Merkle layer: 0 = entity leaf, 1 = scope/cluster, 2 = tenant root.
	Level int `json:"level"`
	// Kind is the node kind ("entity", "scope", "cluster", "tenant").
	Kind string `json:"kind"`
	// Scope is the scope value an L1 scope node summarizes (empty otherwise).
	Scope string `json:"scope,omitempty"`
	// ContentHash is the Merkle freshness key (changes when the subtree changes).
	ContentHash string `json:"contentHash"`
	// SemHash is the 16-hex SimHash fingerprint of the node's summary embedding.
	SemHash string `json:"semHash"`
	// Summary is the node's summary text when surfaced; usually empty in the map
	// (see distillNode docs) — use the node drill-down endpoint for full text.
	Summary string `json:"summary,omitempty"`
}

// distillMapResponse is the payload for GET {BasePath}/distill/map: the tenant
// root id and the full, deterministically-sorted (Level asc, then ID) outline
// of the tenant's context-distillation Merkle tree. Nodes is an empty (non-nil)
// slice when the tenant has no digest data yet.
type distillMapResponse struct {
	// RootID is the stable id of the tenant (L2) root: "digest:2:tenant".
	RootID string `json:"rootId"`
	// Nodes is the outline, sorted Level ascending then ID ascending.
	Nodes []distillNode `json:"nodes"`
}

// distillChild is one immediate child in the node drill-down response. It mirrors
// agent.DigestChild: the child's id, kind, Merkle hashes, and (CAS-backed)
// summary text.
type distillChild struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Summary     string `json:"summary"`
	ContentHash string `json:"contentHash"`
	SemHash     string `json:"semHash"`
}

// distillNodeResponse is the payload for GET {BasePath}/distill/node/:id: the
// target node, its summary text (from CAS; empty when no CAS is configured), and
// its immediate children with their own hashes and summaries.
type distillNodeResponse struct {
	Node     distillNode    `json:"node"`
	Summary  string         `json:"summary"`
	Children []distillChild `json:"children"`
}

// registerDistillRoutes wires the context-distillation (DigestNode Merkle tree)
// read routes onto the given router. They share the same route options
// (auth/tenant middleware) as the rest of the admin surface so the host controls
// the security boundary uniformly.
//
// Routes:
//
//	GET {base}/distill/map       full digest-tree outline for the tenant
//	GET {base}/distill/node/:id  drill into one digest node + its children
//
// Both degrade gracefully when distillation is not configured: when the
// digest_node entity is not registered (no distillation plane), /distill/map
// returns 200 with an empty node list and /distill/node/:id returns 501. A
// configured plane with no built tree simply yields an empty map (200).
func (c *adminController) registerDistillRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	mapOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.distill.map"),
		forge.WithSummary("Context-distillation digest-tree outline for the tenant"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/distill/map", c.handleDistillMap, mapOpts...); err != nil {
		return err
	}

	nodeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.distill.node"),
		forge.WithSummary("Drill into one digest node and its immediate children"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/distill/node/:id", c.handleDistillNode, nodeOpts...)
}

// buildToolkit constructs an agent.Toolkit over the resolved fabric, registry,
// and (optional) CAS. The embedder is intentionally nil: the distillation READ
// endpoints (Map / Digest) never embed — they read digest rows and CAS-backed
// summary text only. A nil CAS means digest summaries come back empty (graceful
// degradation), exactly as agent.Toolkit documents.
func (c *adminController) buildToolkit() (*agent.Toolkit, error) {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return nil, err
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return nil, err
	}
	return agent.NewToolkit(fab, reg, nil, agent.Config{CAS: c.ext.resolveCAS()})
}

// distillConfigured reports whether the instance has the distillation plane
// configured. The signal is registry-derived and side-effect-free: the
// distillation tree lives in the digest_node entity, so the plane is "present"
// exactly when that entity is registered. This matches the Toolkit's own
// behaviour — Map/Digest treat an unregistered digest_node as "no plane".
func (c *adminController) distillConfigured() bool {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return false
	}
	_, ok := reg.Get(agent.DigestEntity)
	return ok
}

// distillNotConfigured returns the 501 response used when the instance has no
// distillation plane wired (no digest_node entity registered). It mirrors the
// not-configured shape used across the admin surface so the SPA can branch on a
// stable error payload.
func (c *adminController) distillNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "distillation plane not configured"})
}

// handleDistillMap serves GET {BasePath}/distill/map.
//
// It returns the full, deterministically-sorted outline of the tenant's
// context-distillation Merkle tree (Toolkit.Map). When the distillation plane is
// not configured (digest_node not registered) it returns 200 with an empty node
// list rather than an error — the map of a tenant with no digest data is simply
// empty, so the SPA renders an empty tree either way.
func (c *adminController) handleDistillMap(ctx forge.Context) error {
	tk, err := c.buildToolkit()
	if err != nil {
		return forge.InternalError(err)
	}

	lines, mapErr := tk.Map(ctx.Request().Context(), agent.MapRequest{})
	if mapErr != nil {
		return forge.InternalError(mapErr)
	}

	nodes := make([]distillNode, 0, len(lines))
	for _, ln := range lines {
		nodes = append(nodes, mapLineToNode(ln))
	}
	return ctx.JSON(http.StatusOK, distillMapResponse{
		RootID: agent.TenantRootID(),
		Nodes:  nodes,
	})
}

// handleDistillNode serves GET {BasePath}/distill/node/:id.
//
// It drills into one digest node (Toolkit.Digest): the node's own outline line,
// its summary text (from CAS; empty when no CAS is configured), and its
// immediate children with their hashes and summaries. Returns 501 when the
// distillation plane is not configured (digest_node not registered) and 404 when
// the node id is absent from the tenant's tree.
func (c *adminController) handleDistillNode(ctx forge.Context) error {
	if !c.distillConfigured() {
		return c.distillNotConfigured(ctx)
	}

	id := strings.TrimSpace(ctx.Param("id"))
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	tk, err := c.buildToolkit()
	if err != nil {
		return forge.InternalError(err)
	}

	view, digestErr := tk.Digest(ctx.Request().Context(), id)
	if digestErr != nil {
		return mapDistillError(ctx, c, digestErr)
	}

	children := make([]distillChild, 0, len(view.Children))
	for _, ch := range view.Children {
		children = append(children, distillChild{
			ID:          ch.ID,
			Kind:        ch.Kind,
			Summary:     ch.Summary,
			ContentHash: ch.ContentHash,
			SemHash:     ch.SemHash,
		})
	}
	return ctx.JSON(http.StatusOK, distillNodeResponse{
		Node:     mapLineToNode(view.Node),
		Summary:  view.Summary,
		Children: children,
	})
}

// mapLineToNode projects an agent.MapLine into the camelCase distillNode wire
// shape.
func mapLineToNode(ln agent.MapLine) distillNode {
	return distillNode{
		ID:          ln.ID,
		Level:       ln.Level,
		Kind:        ln.Kind,
		Scope:       ln.Scope,
		ContentHash: ln.ContentHash,
		SemHash:     ln.SemHash,
		Summary:     ln.Summary,
	}
}

// mapDistillError translates agent.Toolkit.Digest errors to forge HTTP errors:
//
//   - a "not registered" error (digest_node absent) → 501, matching the
//     instance capability — the plane is not configured;
//   - a "not found" error (the id names no node in this tenant's tree) → 404;
//   - everything else → 500.
//
// Toolkit.Digest returns plain fmt errors (no sentinels), so a substring match
// is used to classify the two expected, non-500 cases.
func mapDistillError(ctx forge.Context, c *adminController, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "not registered") {
		return c.distillNotConfigured(ctx)
	}
	if strings.Contains(msg, "not found") {
		return forge.NotFound("digest node not found")
	}
	return forge.InternalError(err)
}
