package query

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// Repo is a type-safe view over one entity, parameterised by its grove
// model T. It is a thin generic layer over RelationalQuerier — the
// interface stays string/any (Go interface methods cannot be generic, and
// the untyped form is what adapters and fakes implement), while Repo gives
// call sites the entity-from-type and typed results:
//
//	repo, _ := query.For[domain.Asset](reg, f.Relational())
//	asset, err := repo.Get(ctx, id)            // *domain.Asset, not any
//	pumps, err := repo.List(ctx, query.ListQuery{Where: []query.Cond{query.Eq("kind", "pump")}})
//
// It adds no query capability beyond the ports — just typing. The graph,
// search and vector queriers are optional; the relational one is required.
type Repo[T any] struct {
	rel      RelationalQuerier
	graph    GraphQuerier
	search   SearchQuerier
	vector   VectorQuerier
	entity   string
	node     string              // graph label (EntitySpec.GraphNode); "" = not graphed
	selfRels map[string]struct{} // declared edges whose Target is this entity
	cache    cache.Cache         // nil = result-set caching off
	qks      cache.Keyspace      // the entity's result-set keyspace (set with cache)
}

// For builds a typed Repo by resolving T's registered entity. T is the
// grove model struct (value or pointer); an unregistered type errors. The
// repo is relational-only until you attach projection queriers via With*
// (fabriq.For wires them all from the facade).
func For[T any](reg *registry.Registry, rel RelationalQuerier) (*Repo[T], error) {
	if reg == nil || rel == nil {
		return nil, fmt.Errorf("fabriq: For needs a registry and a relational querier")
	}
	t := reflect.TypeFor[T]()
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	ent, ok := reg.GetByModelType(t)
	if !ok {
		return nil, fmt.Errorf("fabriq: no registered entity for model type %s", t)
	}
	selfRels := make(map[string]struct{})
	for _, e := range ent.Spec.Edges {
		if e.Target == ent.Spec.Name {
			selfRels[e.Rel] = struct{}{}
		}
	}
	return &Repo[T]{rel: rel, entity: ent.Spec.Name, node: ent.Spec.GraphNode, selfRels: selfRels}, nil
}

// WithGraph attaches the graph querier (enables Traverse).
func (r *Repo[T]) WithGraph(g GraphQuerier) *Repo[T] { r.graph = g; return r }

// WithSearch attaches the search querier (enables Search).
func (r *Repo[T]) WithSearch(s SearchQuerier) *Repo[T] { r.search = s; return r }

// WithVector attaches the vector querier (enables Similar).
func (r *Repo[T]) WithVector(v VectorQuerier) *Repo[T] { r.vector = v; return r }

// WithResultCache enables result-set (id-list) caching for this repo, keyed in
// ks (an entity-keyed Versioned keyspace). Wired by fabriq.For for opted-in
// entities; a repo without it behaves exactly as before.
func (r *Repo[T]) WithResultCache(c cache.Cache, ks cache.Keyspace) *Repo[T] {
	r.cache = c
	r.qks = ks
	return r
}

// Entity returns the resolved registry entity name.
func (r *Repo[T]) Entity() string { return r.entity }

// Get loads one row by id, typed.
func (r *Repo[T]) Get(ctx context.Context, id string) (*T, error) {
	out := new(T)
	if err := r.rel.Get(ctx, r.entity, id, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetMany loads many rows in one batched query, typed; order follows ids,
// missing rows are skipped.
func (r *Repo[T]) GetMany(ctx context.Context, ids []string) ([]*T, error) {
	var out []*T
	if err := r.rel.GetMany(ctx, r.entity, ids, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// List runs a structured query, typed. When result-set caching is enabled for
// the entity, the ordered id-list is cached (Versioned by the entity
// generation, TTL backstop) and hydrated through GetMany.
func (r *Repo[T]) List(ctx context.Context, q ListQuery) ([]*T, error) {
	if r.cache == nil {
		var out []*T
		if err := r.rel.List(ctx, r.entity, q, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	type listFP struct {
		K string
		Q ListQuery
	}
	return r.cachedHydrate(ctx, listFP{K: "list", Q: q}, func(ctx context.Context) ([]string, error) {
		var rows []*T
		if err := r.rel.List(ctx, r.entity, q, &rows); err != nil {
			return nil, err
		}
		return extractIDs(rows)
	})
}

// One fetches the single row matching the given conditions (ANDed) — the
// "load one by something other than id" primitive (e.g. a unique serial):
//
//	pump, err := repo.One(ctx, query.Eq("serial", "SN-777"))
//
// Zero matches is ErrNotFound; more than one is an error (One means one).
// It needs no ListQuery — order and pagination are meaningless for a
// single row — and caps the read at two to detect ambiguity cheaply.
func (r *Repo[T]) One(ctx context.Context, where ...Cond) (*T, error) {
	out, err := r.List(ctx, ListQuery{Where: where, Limit: 2})
	if err != nil {
		return nil, err
	}
	switch len(out) {
	case 0:
		return nil, &fabriqerr.NotFoundError{Entity: r.entity, ID: "(no row matched the filter)"}
	case 1:
		return out[0], nil
	default:
		return nil, fmt.Errorf("fabriq: One matched multiple %s rows; use List", r.entity)
	}
}

// Traverse runs a graph traversal that RETURNs ids and hydrates the full
// rows from Postgres in one batched query — typed, never N+1. The Cypher
// stays raw (the graph's swappability rests on common-subset openCypher,
// not a builder); only the result is typed:
//
//	assets, err := repo.Traverse(ctx,
//	    `MATCH (a:Asset)-[:LOCATED_AT]->(:Site {id:$s}) RETURN a.id`,
//	    map[string]any{"s": siteID})
func (r *Repo[T]) Traverse(ctx context.Context, cypher string, params map[string]any) ([]*T, error) {
	if r.graph == nil {
		return nil, fmt.Errorf("fabriq: Traverse: graph %w (build via fabriq.For)", fabriqerr.ErrStoreNotConfigured)
	}
	if r.cache == nil {
		var out []*T
		if err := r.graph.TraverseAndHydrate(ctx, cypher, params, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	type traverseFP struct {
		K      string
		Cypher string
		Params map[string]any
	}
	return r.cachedHydrate(ctx, traverseFP{K: "traverse", Cypher: cypher, Params: params},
		func(ctx context.Context) ([]string, error) {
			var ids []string
			if err := r.graph.Query(ctx, cypher, params, &ids); err != nil {
				return nil, err
			}
			return ids, nil
		})
}

// Search runs a full-text query against the entity's declared search
// fields, then hydrates the matching rows from Postgres in one batched
// query — typed entities in relevance order, never N+1. It is the one-line
// form of SearchWith; reach for SearchWith when you need filters, sort or
// pagination. For raw hits (highlighting, scores) use f.Search() directly.
func (r *Repo[T]) Search(ctx context.Context, text string, limit int) ([]*T, error) {
	return r.SearchWith(ctx, SearchRequest{Query: text, Limit: limit})
}

// SearchWith runs a structured full-text query — free text plus optional
// non-scoring Filter (the same Cond vocabulary as List), Sort and
// pagination — and hydrates the matches from Postgres in one batched
// query, typed:
//
//	hits, _ := repo.SearchWith(ctx, query.SearchRequest{
//	    Query:  "centrifugal",
//	    Filter: query.Where{query.Eq("kind", "pump"), query.Gte("version", 3)},
//	    Sort:   "name",
//	    Limit:  20,
//	})
//
// Filter and Sort may reference only indexed fields (the declared search
// fields plus id/tenant_id/version). There is no raw engine-DSL form by
// design — full-text search has no portable raw language, so the structured
// query is the whole surface, which keeps the port swappable.
func (r *Repo[T]) SearchWith(ctx context.Context, req SearchRequest) ([]*T, error) {
	if r.search == nil {
		return nil, fmt.Errorf("fabriq: Search: search %w (build via fabriq.For)", fabriqerr.ErrStoreNotConfigured)
	}
	q := SearchQuery{
		Entity: r.entity, Query: req.Query, Filter: req.Filter,
		Sort: req.Sort, Limit: req.Limit, Offset: req.Offset,
	}
	var hits []map[string]any
	if err := r.search.Search(ctx, q, &hits); err != nil {
		return nil, err
	}
	return r.GetMany(ctx, idsFromHits(hits))
}

// Similar runs a vector nearest-neighbour search and hydrates the matched
// rows from Postgres in one batched query — typed entities in similarity
// order, never N+1. For the relevance scores use f.Vector() directly.
func (r *Repo[T]) Similar(ctx context.Context, embedding []float32, k int) ([]*T, error) {
	if r.vector == nil {
		return nil, fmt.Errorf("fabriq: Similar: vector %w (build via fabriq.For)", fabriqerr.ErrStoreNotConfigured)
	}
	var matches []VectorMatch
	if err := r.vector.Similar(ctx, VectorQuery{Entity: r.entity, Embedding: embedding, K: k}, &matches); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(matches))
	for _, m := range matches {
		ids = append(ids, m.ID)
	}
	return r.GetMany(ctx, ids)
}

// idsFromHits pulls the id column out of search documents, preserving
// relevance order.
func idsFromHits(hits []map[string]any) []string {
	ids := make([]string, 0, len(hits))
	for _, h := range hits {
		if id, ok := h[registry.ColumnID].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// graphHopCap bounds variable-length expansion so a typo can't ask the
// engine to walk the whole graph.
const graphHopCap = 16

// relIdent is the portable relationship-type / label identifier grammar
// (matches the adapter's validIdent) — the syntactic injection guard for
// the one place a traversal interpolates an identifier into Cypher.
var relIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// Out returns the same-type neighbours one hop out along a self-edge,
// typed and hydrated from Postgres:
//
//	MATCH (n:Asset {id:$id})-[:CHILD_OF]->(m:Asset) RETURN m.id
//
//	parents, err := repo.Out(ctx, assetID, "CHILD_OF") // []*domain.Asset
//
// rel must be an edge this entity declares whose Target is the entity
// itself (a self-edge) — that is what makes the []*T result sound. Edges
// to other entity types, and anything outside MATCH/edge/RETURN, drop to
// the raw Traverse escape hatch. These helpers emit only the openCypher
// common subset the graphtest conformance suite gates, so they stay
// portable across graph engines.
func (r *Repo[T]) Out(ctx context.Context, id, rel string) ([]*T, error) {
	return r.walk(ctx, id, rel, dirOut, 0, 0)
}

// In is Out with the edge reversed: same-type neighbours one hop in along
// a self-edge — MATCH (n:L {id:$id})<-[:REL]-(m:L) RETURN m.id.
func (r *Repo[T]) In(ctx context.Context, id, rel string) ([]*T, error) {
	return r.walk(ctx, id, rel, dirIn, 0, 0)
}

// Reachable returns the same-type nodes reachable from id by following a
// self-edge between minHops and maxHops times (a variable-length path) —
// the typed ancestors/descendants walk:
//
//	MATCH (n:Asset {id:$id})-[:CHILD_OF*1..3]->(m:Asset) RETURN m.id
//
//	ancestors, err := repo.Reachable(ctx, assetID, "CHILD_OF", 1, 5)
//
// minHops must be >= 1 and maxHops within [minHops, 16]. Ids are deduped
// (multiple paths may reach the same node) before hydration. For the
// reverse direction or richer shapes, use raw Traverse.
func (r *Repo[T]) Reachable(ctx context.Context, id, rel string, minHops, maxHops int) ([]*T, error) {
	return r.walk(ctx, id, rel, dirOut, minHops, maxHops)
}

type walkDir int

const (
	dirOut walkDir = iota
	dirIn
)

// walk validates the traversal, runs the id-returning Cypher, dedupes and
// hydrates in one batched query.
func (r *Repo[T]) walk(ctx context.Context, id, rel string, dir walkDir, minHops, maxHops int) ([]*T, error) {
	cypher, err := r.walkCypher(ctx, rel, dir, minHops, maxHops)
	if err != nil {
		return nil, err
	}
	if r.cache == nil {
		var ids []string
		if err := r.graph.Query(ctx, cypher, map[string]any{"id": id}, &ids); err != nil {
			return nil, fmt.Errorf("fabriq: graph walk: %w", err)
		}
		return r.GetMany(ctx, dedupeStrings(ids))
	}
	type walkFP struct {
		K   string
		Rel string
		Dir walkDir
		Min int
		Max int
		ID  string
	}
	return r.cachedHydrate(ctx, walkFP{K: "walk", Rel: rel, Dir: dir, Min: minHops, Max: maxHops, ID: id},
		func(ctx context.Context) ([]string, error) {
			var ids []string
			if err := r.graph.Query(ctx, cypher, map[string]any{"id": id}, &ids); err != nil {
				return nil, fmt.Errorf("fabriq: graph walk: %w", err)
			}
			return dedupeStrings(ids), nil
		})
}

// walkCypher validates the relationship and builds the common-subset
// Cypher. The label comes from the registry (GraphNode); the relationship
// is checked against the entity's declared self-edges and the identifier
// grammar — so the one interpolation point is injection-safe.
//
// Scope-aware traversals: when ctx carries a secondary scope,
// a WHERE predicate is injected on the matched node (m) so that only
// shared rows (scope_id IS NULL) and same-scope rows (scope_id = $scope)
// are returned. The $scope parameter is bound by the adapter's scopeParams
// at Query time — no explicit param addition is needed here.
// An unscoped context produces no predicate and sees all nodes in the tenant.
func (r *Repo[T]) walkCypher(ctx context.Context, rel string, dir walkDir, minHops, maxHops int) (string, error) {
	if r.graph == nil {
		return "", fmt.Errorf("fabriq: graph traversal: graph %w (build via fabriq.For)", fabriqerr.ErrStoreNotConfigured)
	}
	if r.node == "" {
		return "", fmt.Errorf("fabriq: entity %q is not projected to the graph (no GraphNode)", r.entity)
	}
	if !relIdent.MatchString(rel) {
		return "", fmt.Errorf("fabriq: invalid relationship type %q", rel)
	}
	if _, ok := r.selfRels[rel]; !ok {
		return "", fmt.Errorf("fabriq: %q is not a self-edge on %q; declared self-edges: [%s] (use raw Traverse for cross-type)",
			rel, r.entity, strings.Join(r.selfRelNames(), ", "))
	}
	hop := ""
	if minHops != 0 || maxHops != 0 {
		if minHops < 1 || maxHops < minHops || maxHops > graphHopCap {
			return "", fmt.Errorf("fabriq: hop range %d..%d invalid (need 1 <= min <= max <= %d)", minHops, maxHops, graphHopCap)
		}
		hop = fmt.Sprintf("*%d..%d", minHops, maxHops)
	}
	// Scope predicate: when a secondary scope is set, filter the returned
	// node (m) to shared rows (scope_id IS NULL) or same-scope rows.
	// $scope is injected into params by the adapter's scopeParams at Query
	// time; no explicit param addition is required here.
	scopeWhere := ""
	if _, scoped := tenant.ScopeFromContext(ctx); scoped {
		scopeWhere = " WHERE (m.scope_id IS NULL OR m.scope_id = $scope)"
	}
	switch dir {
	case dirIn:
		return fmt.Sprintf("MATCH (n:%[1]s {id: $id})<-[:%[2]s%[3]s]-(m:%[1]s)%[4]s RETURN m.id ORDER BY m.id", r.node, rel, hop, scopeWhere), nil
	default: // dirOut
		return fmt.Sprintf("MATCH (n:%[1]s {id: $id})-[:%[2]s%[3]s]->(m:%[1]s)%[4]s RETURN m.id ORDER BY m.id", r.node, rel, hop, scopeWhere), nil
	}
}

// selfRelNames returns the declared self-edge relationship types, sorted,
// for error messages.
func (r *Repo[T]) selfRelNames() []string {
	names := make([]string, 0, len(r.selfRels))
	for rel := range r.selfRels {
		names = append(names, rel)
	}
	sort.Strings(names)
	return names
}

// dedupeStrings drops repeats while preserving first-seen order — a
// variable-length path can reach the same node by several routes.
func dedupeStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := in[:0:0]
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
