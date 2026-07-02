package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// GraphNode is one node in a graph subgraph response, as returned by the
// neighbors and traverse endpoints. It mirrors adminapi's graphNode JSON
// exactly: {id, type, label, props}.
type GraphNode struct {
	ID    string         `json:"id"`
	Type  string         `json:"type,omitempty"`
	Label string         `json:"label,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

// GraphEdge is one relationship in a graph subgraph response. It mirrors
// adminapi's graphEdge JSON exactly: {from, to, rel, props}.
type GraphEdge struct {
	From  string         `json:"from"`
	To    string         `json:"to"`
	Rel   string         `json:"rel"`
	Props map[string]any `json:"props,omitempty"`
}

// GraphData is the shared {nodes, edges} payload for the neighbors and
// traverse endpoints. It mirrors adminapi's graphSubgraphResponse JSON
// exactly: {nodes, edges}.
type GraphData struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// GraphQueryResult is the tabular payload for the read-only Cypher
// playground. It mirrors adminapi's graphQueryResponse JSON exactly:
// {columns, rows}. Rows mirror Columns positionally.
type GraphQueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// GraphNeighborsParams are the query parameters for GraphNeighbors. Type and
// ID are required by the backend; Limit is optional.
type GraphNeighborsParams struct {
	// Type is the node's primary label / entity type (e.g. "product").
	Type string
	// ID is the id of the node to expand.
	ID string
	// Limit caps the number of 1-hop relationships expanded (server default
	// 50). Zero omits the query param and defers to the server default.
	Limit int
}

// GraphNeighbors fetches the 1-hop neighbours of a graph node. It calls
// GET {BasePath}/graph/neighbors?type=<type>&id=<id>[&limit=<n>]. Returns
// *APIError with Status 501 when the instance has no graph backend
// configured.
func (c *Client) GraphNeighbors(ctx context.Context, params GraphNeighborsParams) (GraphData, error) {
	q := url.Values{}
	if params.Type != "" {
		q.Set("type", params.Type)
	}
	if params.ID != "" {
		q.Set("id", params.ID)
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}

	var out GraphData
	if err := c.do(ctx, http.MethodGet, "/graph/neighbors", q, nil, &out); err != nil {
		return GraphData{}, err
	}
	return out, nil
}

// GraphTraverseInput is the request body for GraphTraverse. It mirrors
// adminapi's traverseRequest JSON exactly: {type, id, depth, limit}.
type GraphTraverseInput struct {
	// Type is the node's primary label / entity type (e.g. "product").
	Type string `json:"type"`
	// ID is the id of the root node to traverse from.
	ID string `json:"id"`
	// Depth is the BFS hop count (1-3); values outside the range are clamped
	// server-side.
	Depth int `json:"depth"`
	// Limit caps the number of nodes returned (server default/maximum 200).
	Limit int `json:"limit,omitempty"`
}

// GraphTraverse runs a breadth-bounded traversal from a node. It calls
// POST {BasePath}/graph/traverse with body {type, id, depth, limit}. Returns
// *APIError with Status 501 when the instance has no graph backend
// configured.
func (c *Client) GraphTraverse(ctx context.Context, input GraphTraverseInput) (GraphData, error) {
	var out GraphData
	if err := c.do(ctx, http.MethodPost, "/graph/traverse", nil, input, &out); err != nil {
		return GraphData{}, err
	}
	return out, nil
}

// GraphQueryInput is the request body for GraphQuery. It mirrors adminapi's
// graphQueryRequest JSON exactly: {cypher, params}.
type GraphQueryInput struct {
	// Cypher is the read-only openCypher query to run.
	Cypher string `json:"cypher"`
	// Params are optional named parameters referenced as $name in the query.
	Params map[string]any `json:"params,omitempty"`
}

// GraphQuery runs a read-only openCypher query. It calls
// POST {BasePath}/graph/query with body {cypher, params}. A mutating
// statement (CREATE/MERGE/DELETE/SET/REMOVE/DROP) is rejected server-side
// with a 400 *APIError; an unconfigured graph backend yields a 501 *APIError.
func (c *Client) GraphQuery(ctx context.Context, input GraphQueryInput) (GraphQueryResult, error) {
	var out GraphQueryResult
	if err := c.do(ctx, http.MethodPost, "/graph/query", nil, input, &out); err != nil {
		return GraphQueryResult{}, err
	}
	return out, nil
}
