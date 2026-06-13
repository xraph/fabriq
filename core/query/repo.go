package query

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
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
	rel    RelationalQuerier
	graph  GraphQuerier
	search SearchQuerier
	vector VectorQuerier
	entity string
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
	return &Repo[T]{rel: rel, entity: ent.Spec.Name}, nil
}

// WithGraph attaches the graph querier (enables Traverse).
func (r *Repo[T]) WithGraph(g GraphQuerier) *Repo[T] { r.graph = g; return r }

// WithSearch attaches the search querier (enables Search).
func (r *Repo[T]) WithSearch(s SearchQuerier) *Repo[T] { r.search = s; return r }

// WithVector attaches the vector querier (enables Similar).
func (r *Repo[T]) WithVector(v VectorQuerier) *Repo[T] { r.vector = v; return r }

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

// List runs a structured query, typed.
func (r *Repo[T]) List(ctx context.Context, q ListQuery) ([]*T, error) {
	var out []*T
	if err := r.rel.List(ctx, r.entity, q, &out); err != nil {
		return nil, err
	}
	return out, nil
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
	var out []*T
	if err := r.graph.TraverseAndHydrate(ctx, cypher, params, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Search runs a full-text query against the entity's declared search
// fields, then hydrates the matching rows from Postgres in one batched
// query — typed entities in relevance order, never N+1. For raw hits
// (highlighting, scores) use f.Search() directly.
func (r *Repo[T]) Search(ctx context.Context, text string, limit int) ([]*T, error) {
	if r.search == nil {
		return nil, fmt.Errorf("fabriq: Search: search %w (build via fabriq.For)", fabriqerr.ErrStoreNotConfigured)
	}
	var hits []map[string]any
	if err := r.search.Search(ctx, SearchQuery{Entity: r.entity, Query: text, Limit: limit}, &hits); err != nil {
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
