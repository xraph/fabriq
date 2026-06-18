// core/agent/hydrate.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// hydrate loads the given ids of one entity as JSON rows keyed by id. It
// handles typed (Go model) and dynamic (map-native) entities. Missing ids are
// absent from the result.
func (t *Toolkit) hydrate(ctx context.Context, entity string, ids []string) (map[string]json.RawMessage, error) {
	if len(ids) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	ent, ok := t.reg.Get(entity)
	if !ok {
		return nil, fmt.Errorf("agent: unknown entity %q", entity)
	}
	out := make(map[string]json.RawMessage, len(ids))

	if ent.Binding.IsDynamic() {
		var maps []map[string]any
		if err := t.fab.Relational().GetMany(ctx, entity, ids, &maps); err != nil {
			return nil, err
		}
		for _, m := range maps {
			id, _ := m["id"].(string)
			if id == "" {
				continue
			}
			raw, err := json.Marshal(m)
			if err != nil {
				return nil, err
			}
			out[id] = raw
		}
		return out, nil
	}

	mt := ent.Binding.ModelType()
	slicePtr := reflect.New(reflect.SliceOf(mt)) // *[]Model
	if err := t.fab.Relational().GetMany(ctx, entity, ids, slicePtr.Interface()); err != nil {
		return nil, err
	}
	slice := slicePtr.Elem()
	for i := 0; i < slice.Len(); i++ {
		model := slice.Index(i).Interface() // Model (value)
		vals, err := ent.Binding.ValuesByColumn(model)
		if err != nil {
			return nil, err
		}
		id, _ := vals["id"].(string)
		if id == "" {
			continue
		}
		raw, err := json.Marshal(model)
		if err != nil {
			return nil, err
		}
		out[id] = raw
	}
	return out, nil
}
