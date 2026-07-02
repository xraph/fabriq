package client

import (
	"context"
	"net/http"
	"net/url"
)

// VectorMatch is one nearest-neighbour hit, as returned by vector similarity
// search. It mirrors adminapi's vectorMatchItem JSON exactly: {id, score, data}.
type VectorMatch struct {
	ID    string         `json:"id"`
	Score float64        `json:"score"`
	Data  map[string]any `json:"data,omitempty"`
}

// VectorSearchPage is the payload for POST {BasePath}/search/vector. It
// mirrors adminapi's vectorSearchResponse JSON exactly: {matches}.
type VectorSearchPage struct {
	Matches []VectorMatch `json:"matches"`
}

// VectorSearchInput is the request body for SearchVector. It carries one of
// two mutually exclusive modes:
//
//   - TEXT: {type, query, k} — Query is embedded server-side and the
//     resulting vector drives a nearest-neighbour search.
//   - SIMILAR-TO-ENTITY: {type, id, k} — the embedding stored for ID is
//     fetched and used as the query vector.
//
// When both Query and ID are set, ID (similar-to-entity) takes precedence.
type VectorSearchInput struct {
	// Type is the registered dynamic entity type name (e.g. "product").
	Type string `json:"type"`
	// Query is the free-text query for TEXT mode. Requires a configured
	// server-side embedder.
	Query string `json:"query,omitempty"`
	// ID selects SIMILAR-TO-ENTITY mode: find rows similar to this stored
	// embedding.
	ID string `json:"id,omitempty"`
	// K caps the number of returned matches (server default 10).
	K int `json:"k,omitempty"`
	// Filter restricts matches to embeddings whose meta contains all of
	// these key=value pairs (AND-ed). Optional; empty means no meta filter.
	Filter map[string]string `json:"filter,omitempty"`
}

// SearchVector performs a vector similarity search. It calls
// POST {BasePath}/search/vector with body {type, query|id, k, filter}.
func (c *Client) SearchVector(ctx context.Context, input VectorSearchInput) (VectorSearchPage, error) {
	var out VectorSearchPage
	if err := c.do(ctx, http.MethodPost, "/search/vector", nil, input, &out); err != nil {
		return VectorSearchPage{}, err
	}
	return out, nil
}

// VectorEmbeddingInfo is the payload for GET {BasePath}/vector/:entity/:id —
// a read-only inspection of one stored embedding. It mirrors adminapi's
// vectorGetResponse JSON exactly: {entity, id, dims, norm, preview}.
type VectorEmbeddingInfo struct {
	Entity  string    `json:"entity"`
	ID      string    `json:"id"`
	Dims    int       `json:"dims"`
	Norm    float64   `json:"norm"`
	Preview []float32 `json:"preview"`
}

// VectorDeleteResult is the payload for the embedding delete endpoints. It
// mirrors adminapi's vectorDeleteResponse JSON exactly: {deleted}.
type VectorDeleteResult struct {
	Deleted bool `json:"deleted"`
}

// VectorDeleteByMetaInput is the request body for VectorDeleteByMeta. It
// mirrors adminapi's vectorDeleteByMetaRequest JSON exactly:
// {entity, filter, all}.
type VectorDeleteByMetaInput struct {
	// Entity is the registered entity whose embeddings are targeted.
	Entity string `json:"entity"`
	// Filter is an AND-of-equals over embedding meta. An empty filter would
	// delete every embedding for the entity, so it is rejected server-side
	// unless All is set.
	Filter map[string]string `json:"filter,omitempty"`
	// All must be explicitly true to opt into the wipe-all (empty-filter) path.
	All bool `json:"all,omitempty"`
}

// VectorGet inspects a stored embedding (dims, L2 norm, and a leading
// preview). It calls GET {BasePath}/vector/:entity/:id.
func (c *Client) VectorGet(ctx context.Context, entity, id string) (VectorEmbeddingInfo, error) {
	var out VectorEmbeddingInfo
	path := "/vector/" + url.PathEscape(entity) + "/" + url.PathEscape(id)
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return VectorEmbeddingInfo{}, err
	}
	return out, nil
}

// VectorDelete removes one stored embedding (idempotent). It calls
// DELETE {BasePath}/vector/:entity/:id.
func (c *Client) VectorDelete(ctx context.Context, entity, id string) (VectorDeleteResult, error) {
	var out VectorDeleteResult
	path := "/vector/" + url.PathEscape(entity) + "/" + url.PathEscape(id)
	if err := c.do(ctx, http.MethodDelete, path, nil, nil, &out); err != nil {
		return VectorDeleteResult{}, err
	}
	return out, nil
}

// VectorDeleteByMeta removes every embedding for an entity whose meta
// matches the given filter (AND-of-equals). An empty filter is the wipe-all
// path and is rejected server-side unless All is set. It calls
// POST {BasePath}/vector/delete-by-meta with body {entity, filter, all}.
func (c *Client) VectorDeleteByMeta(ctx context.Context, input VectorDeleteByMetaInput) (VectorDeleteResult, error) {
	var out VectorDeleteResult
	if err := c.do(ctx, http.MethodPost, "/vector/delete-by-meta", nil, input, &out); err != nil {
		return VectorDeleteResult{}, err
	}
	return out, nil
}
