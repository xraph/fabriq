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
// It adds no query capability beyond the four port methods — just typing.
type Repo[T any] struct {
	rel    RelationalQuerier
	entity string
}

// For builds a typed Repo by resolving T's registered entity. T is the
// grove model struct (value or pointer); an unregistered type errors.
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
