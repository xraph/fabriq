package adminapi

import (
	"encoding/json"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/registry"
)

// Recall request/response defaults. These mirror agent.Recall's own validation
// (Query required, Budget > 0, at least one Entity) while keeping the HTTP body
// minimal so the SPA can fire a one-field query.
const (
	// defaultRecallBudget is the token budget used when the body omits "budget".
	// It is large enough to surface a useful pack of fused rows for the admin
	// console without truncating most demo results.
	defaultRecallBudget = 2000
	// defaultRecallK is the per-channel candidate count used when the body omits
	// "k". Each channel (vector, search, graph) contributes up to K candidates
	// into the RRF fusion.
	defaultRecallK = 10
)

// recallRequest is the POST {BasePath}/recall body. Only "query" is required;
// every other field defaults (see the default* constants and the Entities
// defaulting in handleRecall).
type recallRequest struct {
	// Query is the free-text recall query. Required; an empty query yields 400.
	Query string `json:"query"`
	// Entities scopes recall to these dynamic entity types. When omitted the
	// handler defaults to every registered dynamic (schema-backed) entity type,
	// so the demo can omit it; pass an explicit list to narrow the search.
	Entities []string `json:"entities,omitempty"`
	// Budget is the token budget for the assembled context pack. Defaults to
	// defaultRecallBudget when omitted or non-positive.
	Budget int `json:"budget,omitempty"`
	// K is the per-channel candidate count fed into RRF fusion. Defaults to
	// defaultRecallK when omitted or non-positive.
	K int `json:"k,omitempty"`
	// Hops is the graph-expansion depth for the graph channel. Defaults to the
	// Toolkit's own default (1) when omitted or non-positive.
	Hops int `json:"hops,omitempty"`
}

// recallItem is one hydrated, fused row in the recall response. It is a stable
// camelCase projection of agent.ContextItem; Row is carried verbatim as a JSON
// object (json.RawMessage), not a quoted/escaped string, so the SPA can render
// the row fields directly. Source lists the channels that contributed the row
// to the fusion ("vector", "search", "graph").
type recallItem struct {
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Row    json.RawMessage `json:"row"`
	Score  float64         `json:"score"`
	Source []string        `json:"source"`
	Tokens int             `json:"tokens"`
}

// recallResponse is the payload for POST {BasePath}/recall: the token-budgeted,
// RRF-fused context pack. Items are ordered best-first by fused score; Omitted
// counts rows that did not fit the budget; Tokens is the total used; Warnings
// carries per-channel degradation notes (e.g. a skipped channel) from the
// lenient recall pipeline.
type recallResponse struct {
	Items    []recallItem `json:"items"`
	Omitted  int          `json:"omitted"`
	Tokens   int          `json:"tokens"`
	Warnings []string     `json:"warnings"`
}

// registerRecallRoutes wires the hybrid-recall route onto the given router. It
// shares the same route options (auth/tenant middleware) as the rest of the
// admin surface so the host controls the security boundary uniformly. Recall is
// tenant-scoped: the underlying Toolkit reads every channel through the
// tenant-stamped request context.
//
// Route:
//
//	POST {base}/recall   RRF-fused vector+search+graph recall → context pack
//
// It degrades gracefully when recall is not configured (no embedder, so the
// vector channel cannot run): the handler returns 501 with a stable error body.
func (c *adminController) registerRecallRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	recallOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.recall"),
		forge.WithSummary("Hybrid recall: RRF fusion of vector, search, and graph channels"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/recall", c.handleRecall, recallOpts...)
}

// recallConfigured reports whether the instance can serve hybrid recall. Recall
// fuses a vector (semantic) channel into the pack, which requires an embedder to
// turn the query text into a vector. Without one the vector channel is skipped
// and recall degenerates to search+graph only; to keep the endpoint's contract
// honest (it is the *hybrid* recall surface) we treat a missing embedder as "not
// configured" and return 501 — matching how the text-mode vector endpoint
// (search.go) gates on the same handle.
func (c *adminController) recallConfigured() bool {
	return c.ext.cfg.Embedder != nil
}

// recallNotConfigured returns the 501 response used when hybrid recall cannot
// run (no embedder wired). It mirrors the not-configured shape used across the
// admin surface so the SPA can branch on a stable error payload.
func (c *adminController) recallNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "recall not configured"})
}

// buildRecallToolkit constructs an agent.Toolkit wired with the configured
// embedder so the vector (semantic) channel runs. It reuses the same fabric +
// registry + CAS resolution as buildToolkit (distill.go); the only difference is
// that recall MUST pass the embedder (distillation reads never embed). The CAS
// is forwarded so digest candidates carry summary text when the distillation
// plane is present, exactly as agent.Recall expects.
func (c *adminController) buildRecallToolkit() (*agent.Toolkit, error) {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return nil, err
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return nil, err
	}
	// VectorDims must match the embedder's dimensionality: NewToolkit rejects a
	// mismatch (its default is 768). Deriving it from the configured embedder lets
	// any embedder (768-dim production, smaller test stubs) drive recall without a
	// separate config knob.
	return agent.NewToolkit(fab, reg, c.ext.cfg.Embedder, agent.Config{
		VectorDims: c.ext.cfg.Embedder.Dims(),
		CAS:        c.ext.resolveCAS(),
	})
}

// defaultRecallEntities returns every registered dynamic (schema-backed) entity
// type, excluding the distillation digest plane (agent.DigestEntity, which the
// recall pipeline probes additively on its own). It is used when the request
// omits "entities" so the demo can fire a bare {query} and still search the full
// dynamic catalogue. Returns nil when no dynamic entity is registered.
func defaultRecallEntities(reg *registry.Registry) []string {
	var names []string
	for _, ent := range reg.All() {
		if ent.Spec.Schema == nil { // dynamic, schema-backed types only
			continue
		}
		if ent.Spec.Name == agent.DigestEntity {
			continue
		}
		names = append(names, ent.Spec.Name)
	}
	return names
}

// handleRecall serves POST {BasePath}/recall.
//
// It runs the agent toolkit's hybrid-recall pipeline: per-channel candidate
// generation (vector via the configured embedder, full-text search, graph
// expansion), Reciprocal Rank Fusion across the channels, authoritative
// relational hydration, and token-budget packing. The result is a camelCase
// context pack whose items carry the contributing channels in "source".
//
// Body: {query, entities?, budget?, k?, hops?}. "query" is required (400 if
// empty). "entities" defaults to every registered dynamic entity type. Returns
// 501 with {"error":"recall not configured"} when no embedder is wired (the
// vector channel — the "hybrid" in hybrid recall — cannot run).
func (c *adminController) handleRecall(ctx forge.Context) error {
	if !c.recallConfigured() {
		return c.recallNotConfigured(ctx)
	}

	var body recallRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&body); decErr != nil {
		return forge.BadRequest("invalid JSON body: " + decErr.Error())
	}
	if body.Query == "" {
		return forge.BadRequest("body field 'query' is required")
	}

	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	entities := body.Entities
	if len(entities) == 0 {
		entities = defaultRecallEntities(reg)
	}
	if len(entities) == 0 {
		// No entities named and none registered: recall has nothing to search.
		return c.recallNotConfigured(ctx)
	}

	budget := body.Budget
	if budget <= 0 {
		budget = defaultRecallBudget
	}
	k := body.K
	if k <= 0 {
		k = defaultRecallK
	}

	tk, err := c.buildRecallToolkit()
	if err != nil {
		return forge.InternalError(err)
	}

	pack, recallErr := tk.Recall(ctx.Request().Context(), agent.RecallRequest{
		Query:    body.Query,
		Entities: entities,
		Budget:   budget,
		K:        k,
		Hops:     body.Hops,
	})
	if recallErr != nil {
		return forge.InternalError(recallErr)
	}

	items := make([]recallItem, 0, len(pack.Items))
	for _, it := range pack.Items {
		// Carry Source as a non-nil slice so it serializes as [] not null.
		src := it.Source
		if src == nil {
			src = []string{}
		}
		items = append(items, recallItem{
			Entity: it.Entity,
			ID:     it.ID,
			Row:    it.Row,
			Score:  it.Score,
			Source: src,
			Tokens: it.Tokens,
		})
	}
	warnings := pack.Warnings
	if warnings == nil {
		warnings = []string{}
	}
	return ctx.JSON(http.StatusOK, recallResponse{
		Items:    items,
		Omitted:  pack.Omitted,
		Tokens:   pack.Tokens,
		Warnings: warnings,
	})
}
