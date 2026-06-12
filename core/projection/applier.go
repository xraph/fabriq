package projection

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// Applier is a pure event-to-mutations function. Implementations must be
// deterministic and side-effect free; all idempotency and dialect concerns
// live in adapters.
type Applier interface {
	Apply(env event.Envelope) ([]Mutation, error)
}

// ApplierFunc adapts a function to the Applier interface.
type ApplierFunc func(env event.Envelope) ([]Mutation, error)

// Apply implements Applier.
func (f ApplierFunc) Apply(env event.Envelope) ([]Mutation, error) { return f(env) }

// GraphApplier derives the graph projection from the registry:
// created/updated -> NodeUpsert + edge maintenance from EdgeSpecs
// (non-empty FK -> EdgeUpsert, empty FK -> EdgeDelete); deleted ->
// NodeDelete. Unknown or non-graph entities produce no mutations.
func GraphApplier(reg *registry.Registry) Applier {
	return ApplierFunc(func(env event.Envelope) ([]Mutation, error) {
		ent, ok := reg.Get(env.Aggregate)
		if !ok || ent.Spec.GraphNode == "" {
			return nil, nil
		}
		verb := eventVerb(env.Type)

		if verb == registry.VerbDeleted {
			return []Mutation{NodeDelete{Label: ent.Spec.GraphNode, ID: env.AggID}}, nil
		}
		if verb != registry.VerbCreated && verb != registry.VerbUpdated {
			return nil, nil
		}

		props, err := decodePayload(env)
		if err != nil {
			return nil, err
		}
		muts := make([]Mutation, 0, 1+len(ent.Spec.Edges))
		muts = append(muts, NodeUpsert{
			Label:   ent.Spec.GraphNode,
			ID:      env.AggID,
			Props:   props,
			Version: env.Version,
		})
		for _, edge := range ent.Spec.Edges {
			target, ok := reg.Get(edge.Target)
			if !ok || target.Spec.GraphNode == "" {
				return nil, fmt.Errorf("fabriq: entity %q edge %s: target %q not registered with a GraphNode",
					env.Aggregate, edge.Rel, edge.Target)
			}
			fk := stringProp(props, edge.Field)
			if fk == "" {
				muts = append(muts, EdgeDelete{Rel: edge.Rel, FromLabel: ent.Spec.GraphNode, FromID: env.AggID, Version: env.Version})
				continue
			}
			muts = append(muts, EdgeUpsert{
				Rel:       edge.Rel,
				FromLabel: ent.Spec.GraphNode,
				FromID:    env.AggID,
				ToLabel:   target.Spec.GraphNode,
				ToID:      fk,
				Version:   env.Version,
			})
		}
		return muts, nil
	})
}

// SearchApplier derives the search projection: created/updated -> DocIndex
// restricted to the declared search fields (plus the structural id,
// tenant_id and version); deleted -> DocDeindex.
func SearchApplier(reg *registry.Registry) Applier {
	return ApplierFunc(func(env event.Envelope) ([]Mutation, error) {
		ent, ok := reg.Get(env.Aggregate)
		if !ok || ent.Spec.Search.Index == "" {
			return nil, nil
		}
		verb := eventVerb(env.Type)

		if verb == registry.VerbDeleted {
			return []Mutation{DocDeindex{Index: ent.Spec.Search.Index, ID: env.AggID}}, nil
		}
		if verb != registry.VerbCreated && verb != registry.VerbUpdated {
			return nil, nil
		}

		props, err := decodePayload(env)
		if err != nil {
			return nil, err
		}
		doc := make(map[string]any, len(ent.Spec.Search.Fields)+3)
		for _, f := range ent.Spec.Search.Fields {
			if v, ok := props[f]; ok {
				doc[f] = v
			}
		}
		doc[registry.ColumnID] = env.AggID
		doc[registry.ColumnTenant] = env.TenantID
		doc[registry.ColumnVersion] = env.Version

		return []Mutation{DocIndex{
			Index:   ent.Spec.Search.Index,
			ID:      env.AggID,
			Doc:     doc,
			Version: env.Version,
		}}, nil
	})
}

func decodePayload(env event.Envelope) (map[string]any, error) {
	props := make(map[string]any)
	if len(env.Payload) == 0 {
		return props, nil
	}
	if err := json.Unmarshal(env.Payload, &props); err != nil {
		return nil, fmt.Errorf("fabriq: event %s payload is not an object: %w", env.ID, err)
	}
	return props, nil
}

func stringProp(props map[string]any, key string) string {
	v, ok := props[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func eventVerb(eventType string) string {
	if i := strings.LastIndexByte(eventType, '.'); i >= 0 {
		return eventType[i+1:]
	}
	return eventType
}
