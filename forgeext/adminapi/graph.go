package adminapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/xraph/forge"
)

// defaultNeighborLimit caps 1-hop neighbour expansion when the caller passes no
// limit.
const defaultNeighborLimit = 50

// maxGraphNodes caps how many nodes a traverse subgraph returns so a deep or
// fan-out traversal cannot return an unbounded payload to the SPA.
const maxGraphNodes = 200

// maxTraverseDepth caps the variable-length traversal depth. openCypher
// variable-length patterns are expensive, so the admin playground hard-limits
// the hop count regardless of what the caller requests.
const maxTraverseDepth = 3

// graphNode is one node in a graph subgraph response. Type is the node's
// primary label (best-effort) and Props carries the scalar node properties the
// query projected.
type graphNode struct {
	ID    string         `json:"id"`
	Type  string         `json:"type,omitempty"`
	Label string         `json:"label,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

// graphEdge is one relationship in a graph subgraph response.
type graphEdge struct {
	From  string         `json:"from"`
	To    string         `json:"to"`
	Rel   string         `json:"rel"`
	Props map[string]any `json:"props,omitempty"`
}

// graphSubgraphResponse is the shared {nodes, edges} payload for the neighbors
// and traverse endpoints. Nodes and edges are deduped by identity.
type graphSubgraphResponse struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

// traverseRequest is the body for POST {BasePath}/graph/traverse.
type traverseRequest struct {
	// Type is the node's primary label / entity type (e.g. "product").
	Type string `json:"type"`
	// ID is the id of the root node to traverse from.
	ID string `json:"id"`
	// Depth is the BFS hop count (1-3); values outside the range are clamped.
	Depth int `json:"depth"`
	// Limit caps the number of nodes returned (default/maximum maxGraphNodes).
	Limit int `json:"limit"`
}

// graphQueryRequest is the body for POST {BasePath}/graph/query — the read-only
// Cypher playground.
type graphQueryRequest struct {
	// Cypher is the read-only openCypher query to run.
	Cypher string `json:"cypher"`
	// Params are optional named parameters referenced as $name in the query.
	Params map[string]any `json:"params"`
}

// graphQueryResponse is the generic columns/rows payload for the Cypher
// playground. Rows mirror Columns positionally.
type graphQueryResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// mutatingCypherKeywords are the clause keywords that make a Cypher statement a
// write. The /graph/query endpoint is read-only (the adapter runs GRAPH.RO_QUERY
// and documents Query as read-only), so a statement containing any of these is
// rejected up front with 400 rather than relying solely on the adapter.
var mutatingCypherKeywords = []string{"CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP"}

// registerGraphRoutes wires the knowledge-graph exploration routes onto the
// given router. They share the same route options (auth/tenant middleware) as
// the rest of the admin surface so the host controls the security boundary
// uniformly, and every read is tenant-scoped: the graph adapter resolves the
// caller's per-tenant graph from the tenant on the request context.
func (c *adminController) registerGraphRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	neighborsOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.graph.neighbors"),
		forge.WithSummary("1-hop neighbours of a graph node (requires ?type=&id=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/graph/neighbors", c.handleGraphNeighbors, neighborsOpts...); err != nil {
		return err
	}

	traverseOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.graph.traverse"),
		forge.WithSummary("BFS subgraph from a node to a bounded depth (body: {type, id, depth, limit})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/graph/traverse", c.handleGraphTraverse, traverseOpts...); err != nil {
		return err
	}

	queryOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.graph.query"),
		forge.WithSummary("Read-only openCypher playground (body: {cypher, params})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/graph/query", c.handleGraphQuery, queryOpts...)
}

// handleGraphNeighbors serves GET {BasePath}/graph/neighbors.
//
// Required query params:
//
//	type  the node's primary label / entity type (e.g. "product")
//	id    the node id to expand
//
// Optional query params:
//
//	limit cap on the number of 1-hop relationships expanded (default 50)
//
// It returns the {nodes, edges} subgraph containing the source node, its 1-hop
// neighbours, and the connecting edges. Returns 400 when type or id is missing
// and 501 when the instance has no graph backend configured.
func (c *adminController) handleGraphNeighbors(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	entityType := ctx.Query("type")
	if entityType == "" {
		return forge.BadRequest("query param 'type' is required")
	}
	id := ctx.Query("id")
	if id == "" {
		return forge.BadRequest("query param 'id' is required")
	}

	limit := defaultNeighborLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, parseErr := strconv.Atoi(lStr)
		if parseErr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxGraphNodes {
			l = maxGraphNodes
		}
		limit = l
	}

	reqCtx := ctx.Request().Context()
	g := fab.Graph()
	if !graphConfigured(reqCtx, g) {
		return c.graphNotConfigured(ctx)
	}

	sub := newSubgraph()

	// Seed the source node first so a node with no relationships still returns
	// itself. The id/label come straight from the request — the node is hydrated
	// with whatever scalar props the neighbour query surfaces (or the source
	// query below) — and a bare node is always present.
	sub.addNode(id, entityType, nil)

	// Hydrate the source node's scalar props (best-effort: a node that does not
	// exist in the graph simply yields no row).
	var srcRows []map[string]any
	srcCypher := "MATCH (n {id: $id}) RETURN n.id AS id, labels(n) AS labels, n.name AS name, n.sku AS sku, n.status AS status LIMIT 1"
	if qErr := g.Query(reqCtx, srcCypher, map[string]any{"id": id}, &srcRows); qErr != nil {
		return mapQueryError(qErr)
	}
	for _, row := range srcRows {
		sub.mergeNodeRow(row["id"], firstLabel(row["labels"]), entityType, scalarProps(row, "name", "sku", "status"))
	}

	// Expand 1-hop neighbours and the connecting edges.
	var rows []map[string]any
	cypher := `MATCH (n {id: $id})-[r]-(m)
RETURN m.id AS nbr_id, labels(m) AS nbr_labels, m.name AS nbr_name, m.sku AS nbr_sku, m.status AS nbr_status,
       type(r) AS rel, startNode(r).id AS from_id, endNode(r).id AS to_id
LIMIT $limit`
	params := map[string]any{"id": id, "limit": limit}
	if qErr := g.Query(reqCtx, cypher, params, &rows); qErr != nil {
		return mapQueryError(qErr)
	}

	for _, row := range rows {
		nbrID := asGraphString(row["nbr_id"])
		if nbrID != "" {
			sub.mergeNodeRow(row["nbr_id"], firstLabel(row["nbr_labels"]), "",
				scalarPropsPrefixed(row, "nbr_", "name", "sku", "status"))
		}
		sub.addEdge(asGraphString(row["from_id"]), asGraphString(row["to_id"]), asGraphString(row["rel"]), nil)
	}

	return ctx.JSON(http.StatusOK, sub.response())
}

// handleGraphTraverse serves POST {BasePath}/graph/traverse.
//
// Request body:
//
//	{ "type": "product", "id": "<id>", "depth": 2, "limit": 200 }
//
// It runs a variable-length traversal to depth (clamped to [1, maxTraverseDepth])
// and returns the deduped {nodes, edges} subgraph, capped at limit nodes
// (default/maximum maxGraphNodes). Returns 400 when type or id is missing and
// 501 when the instance has no graph backend configured.
func (c *adminController) handleGraphTraverse(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req traverseRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Type == "" {
		return forge.BadRequest("field 'type' is required")
	}
	if req.ID == "" {
		return forge.BadRequest("field 'id' is required")
	}

	depth := req.Depth
	if depth < 1 {
		depth = 1
	}
	if depth > maxTraverseDepth {
		depth = maxTraverseDepth
	}

	nodeLimit := maxGraphNodes
	if req.Limit > 0 && req.Limit < maxGraphNodes {
		nodeLimit = req.Limit
	}

	reqCtx := ctx.Request().Context()
	g := fab.Graph()
	if !graphConfigured(reqCtx, g) {
		return c.graphNotConfigured(ctx)
	}

	sub := newSubgraph()
	sub.addNode(req.ID, req.Type, nil)

	// Variable-length traversal. The depth bound is interpolated into the pattern
	// (FalkorDB does not parameterize the range bound) from a server-clamped
	// integer, so it cannot carry injection. Path rows are unrolled into the
	// per-relationship pairs the subgraph builder dedupes.
	cypher := `MATCH p = (n {id: $id})-[*1..` + strconv.Itoa(depth) + `]-(m)
WITH relationships(p) AS rels
UNWIND rels AS r
RETURN DISTINCT startNode(r).id AS from_id, labels(startNode(r)) AS from_labels, startNode(r).name AS from_name,
       endNode(r).id AS to_id, labels(endNode(r)) AS to_labels, endNode(r).name AS to_name,
       type(r) AS rel
LIMIT $limit`
	params := map[string]any{"id": req.ID, "limit": nodeLimit * maxTraverseDepth}

	var rows []map[string]any
	if qErr := g.Query(reqCtx, cypher, params, &rows); qErr != nil {
		return mapQueryError(qErr)
	}

	for _, row := range rows {
		if len(sub.nodeIndex) >= nodeLimit {
			break
		}
		fromID := asGraphString(row["from_id"])
		toID := asGraphString(row["to_id"])
		if fromID != "" {
			sub.mergeNodeRow(row["from_id"], firstLabel(row["from_labels"]), "",
				scalarPropsPrefixed(row, "from_", "name"))
		}
		if toID != "" {
			sub.mergeNodeRow(row["to_id"], firstLabel(row["to_labels"]), "",
				scalarPropsPrefixed(row, "to_", "name"))
		}
		sub.addEdge(fromID, toID, asGraphString(row["rel"]), nil)
	}

	return ctx.JSON(http.StatusOK, sub.response())
}

// handleGraphQuery serves POST {BasePath}/graph/query — the read-only Cypher
// playground.
//
// Request body:
//
//	{ "cypher": "MATCH (n) RETURN n.id LIMIT 5", "params": {"x": 1} }
//
// The adapter's Query is read-only (GRAPH.RO_QUERY); this handler additionally
// rejects statements that contain an obviously-mutating clause keyword
// (CREATE/MERGE/DELETE/SET/REMOVE/DROP) with 400 before issuing the query.
// Returns 400 for an empty or mutating query and 501 when the instance has no
// graph backend configured.
func (c *adminController) handleGraphQuery(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req graphQueryRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	cypher := strings.TrimSpace(req.Cypher)
	if cypher == "" {
		return forge.BadRequest("field 'cypher' is required")
	}
	if containsMutatingClause(cypher) {
		return forge.BadRequest("read-only queries only: CREATE/MERGE/DELETE/SET/REMOVE/DROP are not permitted")
	}

	reqCtx := ctx.Request().Context()
	g := fab.Graph()
	if !graphConfigured(reqCtx, g) {
		return c.graphNotConfigured(ctx)
	}

	// The adapter scans column-keyed rows into *[]map[string]any. We reshape that
	// into a stable columns/rows envelope: the column order is taken from the
	// first row's keys (sorted for determinism) and every row is projected in
	// that order so the playground renders a uniform table.
	var rows []map[string]any
	if qErr := g.Query(reqCtx, cypher, req.Params, &rows); qErr != nil {
		return mapQueryError(qErr)
	}

	columns := columnOrder(rows)
	out := make([][]any, 0, len(rows))
	for _, row := range rows {
		cells := make([]any, len(columns))
		for i, col := range columns {
			cells[i] = row[col]
		}
		out = append(out, cells)
	}

	return ctx.JSON(http.StatusOK, graphQueryResponse{Columns: columns, Rows: out})
}

// graphNotConfigured returns the 501 response used when the instance has no
// graph backend wired. It mirrors the not-configured shape used across the admin
// surface so the SPA can branch on a stable error payload.
func (c *adminController) graphNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "graph not configured"})
}

// containsMutatingClause reports whether the (case-insensitive) Cypher text
// contains any mutating clause keyword. It is a conservative substring guard:
// false positives (e.g. a "CREATE" substring inside a string literal) are
// acceptable for a read-only playground.
func containsMutatingClause(cypher string) bool {
	upper := strings.ToUpper(cypher)
	for _, kw := range mutatingCypherKeywords {
		if strings.Contains(upper, kw) {
			return true
		}
	}
	return false
}

// columnOrder derives a stable, sorted column list from the union of keys across
// rows. Sorting keeps the playground table deterministic across requests.
func columnOrder(rows []map[string]any) []string {
	seen := map[string]struct{}{}
	for _, row := range rows {
		for k := range row {
			seen[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(seen))
	for k := range seen {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// --- subgraph accumulator ----------------------------------------------------

// subgraph accumulates deduped nodes and edges as query rows are scanned.
type subgraph struct {
	nodes     []graphNode
	nodeIndex map[string]int // node id -> index in nodes
	edges     []graphEdge
	edgeSeen  map[string]struct{}
}

func newSubgraph() *subgraph {
	return &subgraph{nodeIndex: map[string]int{}, edgeSeen: map[string]struct{}{}}
}

// addNode inserts a node by id if absent, recording its type/label. It does not
// overwrite an existing node's type with an empty one.
func (s *subgraph) addNode(id, typ string, props map[string]any) {
	if id == "" {
		return
	}
	if idx, ok := s.nodeIndex[id]; ok {
		if typ != "" && s.nodes[idx].Type == "" {
			s.nodes[idx].Type = typ
			s.nodes[idx].Label = typ
		}
		if len(props) > 0 {
			s.mergeProps(idx, props)
		}
		return
	}
	s.nodeIndex[id] = len(s.nodes)
	s.nodes = append(s.nodes, graphNode{ID: id, Type: typ, Label: typ, Props: props})
}

// mergeNodeRow merges a scanned node row: id from the raw cell, label from the
// graph (falling back to fallbackType), and scalar props.
func (s *subgraph) mergeNodeRow(rawID any, graphLabel, fallbackType string, props map[string]any) {
	id := asGraphString(rawID)
	if id == "" {
		return
	}
	typ := graphLabel
	if typ == "" {
		typ = fallbackType
	}
	s.addNode(id, typ, props)
}

// mergeProps merges props into an existing node, not overwriting set values with
// nil.
func (s *subgraph) mergeProps(idx int, props map[string]any) {
	if s.nodes[idx].Props == nil {
		s.nodes[idx].Props = map[string]any{}
	}
	for k, v := range props {
		if v == nil {
			continue
		}
		s.nodes[idx].Props[k] = v
	}
}

// addEdge inserts an edge if its (from, rel, to) triple is unseen.
func (s *subgraph) addEdge(from, to, rel string, props map[string]any) {
	if from == "" || to == "" || rel == "" {
		return
	}
	key := from + "\x00" + rel + "\x00" + to
	if _, ok := s.edgeSeen[key]; ok {
		return
	}
	s.edgeSeen[key] = struct{}{}
	s.edges = append(s.edges, graphEdge{From: from, To: to, Rel: rel, Props: props})
}

// response renders the accumulated subgraph, ensuring non-nil slices for a
// stable JSON shape.
func (s *subgraph) response() graphSubgraphResponse {
	if s.nodes == nil {
		s.nodes = []graphNode{}
	}
	if s.edges == nil {
		s.edges = []graphEdge{}
	}
	return graphSubgraphResponse{Nodes: s.nodes, Edges: s.edges}
}

// --- row scalar helpers ------------------------------------------------------

// asGraphString coerces a scanned graph cell to a string. FalkorDB returns
// scalars as strings/ints; non-strings are formatted, nil becomes "".
func asGraphString(v any) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return strconv.FormatInt(toInt64Graph(val), 10)
	}
}

// toInt64Graph coerces an integer-ish cell to int64 (best-effort; 0 otherwise).
func toInt64Graph(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// firstLabel extracts the primary label from a labels() cell. The FalkorDB
// adapter decodes a node's label list and stringifies it, so this cell arrives
// as a Go-formatted slice string like "[Product]" (or "[A B]" for multi-label
// nodes); it may also arrive as a real []any/[]string slice from other engines.
// This returns the first label, brackets stripped.
func firstLabel(v any) string {
	switch labels := v.(type) {
	case []any:
		if len(labels) > 0 {
			return asGraphString(labels[0])
		}
	case []string:
		if len(labels) > 0 {
			return labels[0]
		}
	case string:
		s := strings.TrimSpace(labels)
		s = strings.TrimPrefix(s, "[")
		s = strings.TrimSuffix(s, "]")
		if idx := strings.IndexByte(s, ' '); idx >= 0 {
			s = s[:idx] // first label only for a multi-label "[A B]" form
		}
		return s
	}
	return ""
}

// scalarProps builds a props map from the named columns of a row, dropping nil
// values.
func scalarProps(row map[string]any, keys ...string) map[string]any {
	props := map[string]any{}
	for _, k := range keys {
		if v, ok := row[k]; ok && v != nil {
			props[k] = v
		}
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

// scalarPropsPrefixed builds a props map keyed by the bare name from prefixed
// columns (e.g. prefix "nbr_" + key "name" reads "nbr_name" into "name").
func scalarPropsPrefixed(row map[string]any, prefix string, keys ...string) map[string]any {
	props := map[string]any{}
	for _, k := range keys {
		if v, ok := row[prefix+k]; ok && v != nil {
			props[k] = v
		}
	}
	if len(props) == 0 {
		return nil
	}
	return props
}
