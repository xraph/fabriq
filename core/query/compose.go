package query

import (
	"context"
	"fmt"
	"reflect"

	"github.com/xraph/fabriq/core/registry"
)

// TraverseAndHydrate is the composed graph→relational read: run a Cypher
// traversal that RETURNs aggregate ids (single column), then hydrate the
// full rows from Postgres in exactly ONE batched relational query. The
// entity is inferred from into's element type via the registry, so the
// call site stays Graph().TraverseAndHydrate(ctx, cypher, params, &assets).
//
// Adapters implement GraphQuerier.TraverseAndHydrate by delegating here.
func TraverseAndHydrate(ctx context.Context, reg *registry.Registry, g GraphQuerier, rel RelationalQuerier,
	cypher string, params map[string]any, into any,
) error {
	ent, err := entityForSlice(reg, into)
	if err != nil {
		return err
	}

	var ids []string
	if err := g.Query(ctx, cypher, params, &ids); err != nil {
		return fmt.Errorf("fabriq: traverse: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}
	if err := rel.GetMany(ctx, ent.Spec.Name, ids, into); err != nil {
		return fmt.Errorf("fabriq: hydrate %s: %w", ent.Spec.Name, err)
	}
	return nil
}

// entityForSlice resolves the registry entity whose model matches into's
// element type. into must be a pointer to a slice of models (pointer or
// value elements).
func entityForSlice(reg *registry.Registry, into any) (*registry.Entity, error) {
	t := reflect.TypeOf(into)
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Slice {
		return nil, fmt.Errorf("fabriq: hydration target must be a pointer to slice, got %T", into)
	}
	elem := t.Elem().Elem()
	for elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	ent, ok := reg.GetByModelType(elem)
	if !ok {
		return nil, fmt.Errorf("fabriq: no registered entity for model type %s", elem)
	}
	return ent, nil
}
