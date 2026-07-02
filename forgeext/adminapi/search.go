package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
)

// errEmbedderEmpty is returned when a configured embedder yields no vectors for
// a non-empty query — a contract violation surfaced as a 500.
var errEmbedderEmpty = errors.New("fabriq-admin-api: embedder returned no vectors")

// defaultSearchLimit is the default page size for full-text search results.
const defaultSearchLimit = 25

// defaultVectorK is the default neighbour count for vector similarity search.
const defaultVectorK = 10

// searchItem is one full-text search hit. Data carries the indexed document as
// returned by the search projection (the declared search fields plus the
// structural id/tenant/version columns).
type searchItem struct {
	ID   string         `json:"id"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

// searchResponse is the payload for GET {BasePath}/search.
type searchResponse struct {
	Items []searchItem `json:"items"`
}

// vectorSearchRequest is the request body for POST {BasePath}/search/vector.
//
// It carries one of two mutually exclusive modes:
//
//   - TEXT: {type, query, k} — Query is embedded via the configured Embedder
//     and the resulting vector drives a nearest-neighbour search.
//   - SIMILAR-TO-ENTITY: {type, id, k} — the embedding stored for ID is fetched
//     and used as the query vector (no embedder required).
//
// When both Query and ID are present, ID (similar-to-entity) takes precedence
// because it needs no embedder and is always available.
type vectorSearchRequest struct {
	// Type is the registered dynamic entity type name (e.g. "product").
	Type string `json:"type"`
	// Query is the free-text query for TEXT mode. Requires a configured Embedder.
	Query string `json:"query"`
	// ID selects SIMILAR-TO-ENTITY mode: find rows similar to this stored embedding.
	ID string `json:"id"`
	// K caps the number of returned matches (default 10).
	K int `json:"k"`
	// Filter restricts matches to embeddings whose meta contains all of these
	// key=value pairs (AND-ed). Optional; empty means no meta filter.
	Filter map[string]string `json:"filter,omitempty"`
}

// vectorMatchItem is one nearest-neighbour hit. Data is hydrated best-effort
// from the relational source of truth and may be nil when the row could not be
// loaded (e.g. it was deleted after indexing).
type vectorMatchItem struct {
	ID    string         `json:"id"`
	Score float64        `json:"score"`
	Data  map[string]any `json:"data,omitempty"`
}

// vectorSearchResponse is the payload for POST {BasePath}/search/vector.
type vectorSearchResponse struct {
	Matches []vectorMatchItem `json:"matches"`
}

// registerSearchRoutes wires the full-text and vector-similarity search routes
// onto the given router. They share the same route options (auth/tenant
// middleware) as the rest of the admin surface so the host controls the
// security boundary uniformly.
func (c *adminController) registerSearchRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	searchOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.search"),
		forge.WithSummary("Full-text search over an entity's indexed fields (requires ?type=&q=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/search", c.handleSearch, searchOpts...); err != nil {
		return err
	}

	vectorOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.search.vector"),
		forge.WithSummary("Vector similarity search (body: {type, query|id, k})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/search/vector", c.handleVectorSearch, vectorOpts...)
}

// handleSearch serves GET {BasePath}/search.
//
// Required query params:
//
//	type  entity type name (must be search-indexed)
//	q     full-text query string
//
// Optional query params:
//
//	limit page size (default 25, capped at maxLimit)
//
// Returns 501 when the instance has no search backend configured, and 400 when
// type or q is missing.
func (c *adminController) handleSearch(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	entityType := ctx.Query("type")
	if entityType == "" {
		return forge.BadRequest("query param 'type' is required")
	}
	q := ctx.Query("q")
	if q == "" {
		return forge.BadRequest("query param 'q' is required")
	}

	limit := defaultSearchLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, parseErr := strconv.Atoi(lStr)
		if parseErr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxLimit {
			l = maxLimit
		}
		limit = l
	}

	// Optional offset for pagination.
	offset := 0
	if oStr := ctx.Query("offset"); oStr != "" {
		o, parseErr := strconv.Atoi(oStr)
		if parseErr != nil || o < 0 {
			return forge.BadRequest("query param 'offset' must be a non-negative integer")
		}
		offset = o
	}

	// Optional sort: an indexed column, optionally suffixed " DESC" (empty =
	// sort by relevance score). Validated against indexed fields by the adapter.
	sort := strings.TrimSpace(ctx.Query("sort"))

	// Optional equality filters: repeated ?filter=field:value, AND-ed. Mirrors
	// the live endpoint's map→Eqs pattern; columns must be indexed fields (the
	// adapter validates and errors otherwise). Values are bound, never inlined.
	var filter query.Where
	for _, raw := range ctx.Request().URL.Query()["filter"] {
		field, val, ok := strings.Cut(raw, ":")
		field = strings.TrimSpace(field)
		if !ok || field == "" {
			return forge.BadRequest("query param 'filter' must be 'field:value'")
		}
		filter = append(filter, query.Eq(field, strings.TrimSpace(val)))
	}

	reqCtx := ctx.Request().Context()
	searcher := fab.Search()

	// Detect an unconfigured search backend BEFORE issuing the real query: the
	// notConfigured stub answers every Search with ErrStoreNotConfigured. Reuse
	// the same tenant-less, side-effect-free probe the capabilities endpoint uses.
	if !searchConfigured(reqCtx, searcher) {
		return c.searchNotConfigured(ctx)
	}

	var rows []map[string]any
	sq := query.SearchQuery{Entity: entityType, Query: q, Filter: filter, Sort: sort, Limit: limit, Offset: offset}
	if searchErr := searcher.Search(reqCtx, sq, &rows); searchErr != nil {
		return renderError(ctx, searchErr)
	}

	items := make([]searchItem, 0, len(rows))
	for _, row := range rows {
		id, _ := row["id"].(string)
		items = append(items, searchItem{ID: id, Type: entityType, Data: row})
	}
	return ctx.JSON(http.StatusOK, searchResponse{Items: items})
}

// handleVectorSearch serves POST {BasePath}/search/vector.
//
// Request body (one of two modes):
//
//	{ "type": "<entityName>", "query": "<text>", "k": 10 }   // TEXT (needs embedder)
//	{ "type": "<entityName>", "id":    "<rowId>", "k": 10 }   // SIMILAR-TO-ENTITY
//
// Returns 501 when the instance has no vector backend configured, 501 for a
// TEXT query with no embedder configured, and 400 when type is missing or
// neither query nor id is supplied.
func (c *adminController) handleVectorSearch(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req vectorSearchRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Type == "" {
		return forge.BadRequest("field 'type' is required")
	}
	if req.Query == "" && req.ID == "" {
		return forge.BadRequest("one of 'query' (text) or 'id' (similar-to-entity) is required")
	}

	k := defaultVectorK
	if req.K > 0 {
		k = req.K
	}

	reqCtx := ctx.Request().Context()
	vec := fab.Vector()

	// Detect an unconfigured vector backend BEFORE any embedding work: the
	// notConfigured stub answers Get/Similar with ErrStoreNotConfigured. Reuse
	// the same tenant-less, side-effect-free probe the capabilities endpoint uses.
	if !vectorConfigured(reqCtx, vec) {
		return c.vectorNotConfigured(ctx)
	}

	// Resolve the query embedding from whichever mode the caller selected.
	// SIMILAR-TO-ENTITY takes precedence: it needs no embedder.
	var embedding []float32
	switch {
	case req.ID != "":
		stored, getErr := vec.Get(reqCtx, req.Type, req.ID)
		if getErr != nil {
			return renderError(ctx, getErr)
		}
		embedding = stored
	default: // TEXT mode (req.Query != "")
		emb := c.ext.cfg.Embedder
		if emb == nil {
			return ctx.JSON(http.StatusNotImplemented, map[string]string{
				"error": "no embedder configured for text query; pass {id} for similar-to-entity",
			})
		}
		vectors, embErr := emb.Embed(reqCtx, []string{req.Query})
		if embErr != nil {
			return renderError(ctx, embErr)
		}
		if len(vectors) == 0 {
			return renderError(ctx, errEmbedderEmpty)
		}
		embedding = vectors[0]
	}

	var matches []query.VectorMatch
	vq := query.VectorQuery{Entity: req.Type, Embedding: embedding, K: k, Filter: req.Filter}
	if simErr := vec.Similar(reqCtx, vq, &matches); simErr != nil {
		return renderError(ctx, simErr)
	}

	items := c.hydrateMatches(reqCtx, fab, req.Type, matches)
	return ctx.JSON(http.StatusOK, vectorSearchResponse{Matches: items})
}

// hydrateMatches loads the relational row for each vector match best-effort.
// A row that cannot be loaded (deleted, or an unknown type) yields a match with
// nil Data rather than failing the whole request — the scores are still useful
// to a search playground. Hydration reuses the map-native List path the rest of
// the admin surface uses for dynamic entities.
func (c *adminController) hydrateMatches(
	ctx context.Context, fab query.Fabric, entityType string, matches []query.VectorMatch,
) []vectorMatchItem {
	items := make([]vectorMatchItem, 0, len(matches))
	for _, m := range matches {
		item := vectorMatchItem{ID: m.ID, Score: m.Score}
		var rows []map[string]any
		q := query.ListQuery{Where: query.Where{query.Eq("id", m.ID)}, Limit: 1}
		if err := fab.Relational().List(ctx, entityType, q, &rows); err == nil && len(rows) > 0 {
			item.Data = rows[0]
		}
		items = append(items, item)
	}
	return items
}

// searchNotConfigured returns the 501 response used when the instance has no
// search backend wired. It mirrors the not-configured shape used across the
// admin surface so the SPA can branch on a stable error payload.
func (c *adminController) searchNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "search not configured"})
}

// vectorNotConfigured returns the 501 response used when the instance has no
// vector backend wired.
func (c *adminController) vectorNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "vector not configured"})
}
