package registry

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// Entity is a registered, compiled spec: the declarative EntitySpec plus
// its relational Binding.
type Entity struct {
	Spec    EntitySpec
	Binding *Binding
}

// Registry holds all registered entities. Registration happens at startup;
// lookups are concurrent and read-only afterwards.
type Registry struct {
	mu       sync.RWMutex
	entities map[string]*Entity
	byModel  map[reflect.Type]*Entity
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		entities: make(map[string]*Entity),
		byModel:  make(map[reflect.Type]*Entity),
	}
}

// validateAndBind runs all spec validation and computes the Binding. It takes
// no lock and does not mutate the registry — the shared prep for Register and
// Replace.
func (r *Registry) validateAndBind(spec EntitySpec) (*Binding, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("fabriq: entity spec has empty name")
	}
	if spec.Kind == KindDocument && spec.CRDT == nil {
		return nil, fmt.Errorf("fabriq: entity %q: KindDocument requires a CRDT spec", spec.Name)
	}

	binding, err := bind(spec)
	if err != nil {
		return nil, err
	}

	if spec.GraphNode == "" && len(spec.Edges) > 0 {
		return nil, fmt.Errorf("fabriq: entity %q: edges declared but GraphNode is empty", spec.Name)
	}
	for _, e := range spec.Edges {
		if e.Field == "" || e.Rel == "" || e.Target == "" {
			return nil, fmt.Errorf("fabriq: entity %q: edge %+v: field, rel and target are all required", spec.Name, e)
		}
		if !binding.HasColumn(e.Field) {
			return nil, fmt.Errorf("fabriq: entity %q: edge field %q is not a column of %s", spec.Name, e.Field, binding.Table)
		}
	}
	for _, f := range spec.Search.Fields {
		if !binding.HasColumn(f) {
			return nil, fmt.Errorf("fabriq: entity %q: search field %q is not a column of %s", spec.Name, f, binding.Table)
		}
	}
	if spec.Search.Index == "" && len(spec.Search.Fields) > 0 {
		return nil, fmt.Errorf("fabriq: entity %q: search fields declared but index name is empty", spec.Name)
	}
	if spec.GraphEdge != nil {
		if spec.GraphNode != "" || len(spec.Edges) > 0 {
			return nil, fmt.Errorf("fabriq: entity %q: GraphEdge is exclusive with GraphNode/Edges", spec.Name)
		}
		ge := spec.GraphEdge
		if ge.SourceLabel == "" || ge.TargetLabel == "" {
			return nil, fmt.Errorf("fabriq: entity %q: GraphEdge needs SourceLabel and TargetLabel", spec.Name)
		}
		for _, f := range []string{ge.TypeField, ge.SourceField, ge.TargetField} {
			if f == "" || !binding.HasColumn(f) {
				return nil, fmt.Errorf("fabriq: entity %q: GraphEdge field %q is not a column of %s", spec.Name, f, binding.Table)
			}
		}
		structural := map[string]bool{ge.TypeField: true, ge.SourceField: true, ge.TargetField: true}
		for _, f := range ge.PropFields {
			if structural[f] {
				return nil, fmt.Errorf("fabriq: entity %q: GraphEdge PropField %q shadows a structural field", spec.Name, f)
			}
		}
		for _, f := range ge.PropFields {
			if !binding.HasColumn(f) {
				return nil, fmt.Errorf("fabriq: entity %q: GraphEdge prop %q is not a column of %s", spec.Name, f, binding.Table)
			}
		}
	}
	for _, s := range spec.Subscribe {
		if s.Name == "" {
			return nil, fmt.Errorf("fabriq: entity %q: subscription scope with empty name", spec.Name)
		}
		if s.Field != "" && !binding.HasColumn(s.Field) {
			return nil, fmt.Errorf("fabriq: entity %q: scope %q field %q is not a column of %s", spec.Name, s.Name, s.Field, binding.Table)
		}
	}
	if spec.Live != nil {
		for _, c := range spec.Live.Sortable {
			if !binding.HasColumn(c) {
				return nil, fmt.Errorf("fabriq: entity %q: live sortable column %q is not a column of %s", spec.Name, c, binding.Table)
			}
		}
		for _, c := range spec.Live.Filterable {
			if !binding.HasColumn(c) {
				return nil, fmt.Errorf("fabriq: entity %q: live filterable column %q is not a column of %s", spec.Name, c, binding.Table)
			}
		}
	}

	return binding, nil
}

// Register compiles and validates a spec. Cross-entity references (edge
// targets) are checked in Validate once all entities are registered.
func (r *Registry) Register(spec EntitySpec) error {
	binding, err := r.validateAndBind(spec)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.entities[spec.Name]; dup {
		return fmt.Errorf("fabriq: entity %q registered twice", spec.Name)
	}
	// Dynamic entities have no Go model type; skip the byModel index.
	if mt := binding.ModelType(); mt != nil {
		if prev, dup := r.byModel[mt]; dup {
			return fmt.Errorf("fabriq: model type %s already bound to entity %q", mt, prev.Spec.Name)
		}
		ent := &Entity{Spec: spec, Binding: binding}
		r.entities[spec.Name] = ent
		r.byModel[mt] = ent
		return nil
	}
	ent := &Entity{Spec: spec, Binding: binding}
	r.entities[spec.Name] = ent
	return nil
}

// Replace re-registers an existing DYNAMIC entity with an updated spec,
// recomputing its Binding. It is the runtime-mutation counterpart to Register
// (which rejects duplicates). Replacing an unknown entity, or a modelled
// (static) entity, is an error — statics are compile-time and keep the
// "register before Open" contract.
func (r *Registry) Replace(spec EntitySpec) error {
	binding, err := r.validateAndBind(spec)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.entities[spec.Name]
	if !ok {
		return fmt.Errorf("fabriq: cannot replace unknown entity %q", spec.Name)
	}
	if cur.Spec.Schema == nil {
		return fmt.Errorf("fabriq: entity %q is not dynamic; cannot replace at runtime", spec.Name)
	}
	if binding.ModelType() != nil {
		return fmt.Errorf("fabriq: replacement spec for %q is modelled; dynamic-only", spec.Name)
	}
	r.entities[spec.Name] = &Entity{Spec: spec, Binding: binding}
	return nil
}

// Unregister removes a DYNAMIC entity from the registry. Removing an unknown
// or modelled entity is an error.
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.entities[name]
	if !ok {
		return fmt.Errorf("fabriq: cannot unregister unknown entity %q", name)
	}
	if cur.Spec.Schema == nil {
		return fmt.Errorf("fabriq: entity %q is not dynamic; cannot unregister at runtime", name)
	}
	delete(r.entities, name)
	if mt := cur.Binding.ModelType(); mt != nil {
		delete(r.byModel, mt)
	}
	return nil
}

// GetByModelType returns the entity bound to the given model struct type;
// it powers hydration-target inference in TraverseAndHydrate.
func (r *Registry) GetByModelType(t reflect.Type) (*Entity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byModel[t]
	return e, ok
}

// MustRegister is Register that panics; for static wiring in domain packs.
func (r *Registry) MustRegister(spec EntitySpec) {
	if err := r.Register(spec); err != nil {
		panic(err)
	}
}

// Get returns the entity registered under name.
func (r *Registry) Get(name string) (*Entity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entities[name]
	return e, ok
}

// All returns every registered entity, sorted by name for determinism.
func (r *Registry) All() []*Entity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Entity, 0, len(r.entities))
	for _, e := range r.entities {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.Name < out[j].Spec.Name })
	return out
}

// Validate performs startup validation across entities: every edge target
// must itself be registered. Call once after all Register calls.
func (r *Registry) Validate() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for name, e := range r.entities {
		for _, edge := range e.Spec.Edges {
			target, ok := r.entities[edge.Target]
			if !ok {
				return fmt.Errorf("fabriq: entity %q: edge %s targets unregistered entity %q", name, edge.Rel, edge.Target)
			}
			if target.Spec.GraphNode == "" {
				return fmt.Errorf("fabriq: entity %q: edge %s targets %q which has no GraphNode", name, edge.Rel, edge.Target)
			}
		}
		if e.Spec.Embed != nil && e.Spec.Embed.Text == nil {
			if len(e.Spec.Embed.Fields) == 0 {
				return fmt.Errorf("fabriq: entity %q: Embed needs Fields or Text", name)
			}
			for _, f := range e.Spec.Embed.Fields {
				if !e.Binding.HasColumn(f) {
					return fmt.Errorf("fabriq: entity %q: Embed field %q is not a column", name, f)
				}
			}
		}
		if e.Spec.Distill != nil && e.Spec.Distill.Text == nil {
			if len(e.Spec.Distill.SourceFields) == 0 {
				return fmt.Errorf("fabriq: entity %q: Distill needs SourceFields or Text", name)
			}
			for _, f := range e.Spec.Distill.SourceFields {
				if !e.Binding.HasColumn(f) {
					return fmt.Errorf("fabriq: entity %q: Distill field %q is not a column", name, f)
				}
			}
		}
	}
	return nil
}
