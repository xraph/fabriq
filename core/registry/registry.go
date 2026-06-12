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

// Register compiles and validates a spec. Cross-entity references (edge
// targets) are checked in Validate once all entities are registered.
func (r *Registry) Register(spec EntitySpec) error {
	if spec.Name == "" {
		return fmt.Errorf("fabriq: entity spec has empty name")
	}
	if spec.Kind == KindDocument && spec.CRDT == nil {
		return fmt.Errorf("fabriq: entity %q: KindDocument requires a CRDT spec", spec.Name)
	}

	binding, err := bind(spec)
	if err != nil {
		return err
	}

	if spec.GraphNode == "" && len(spec.Edges) > 0 {
		return fmt.Errorf("fabriq: entity %q: edges declared but GraphNode is empty", spec.Name)
	}
	for _, e := range spec.Edges {
		if e.Field == "" || e.Rel == "" || e.Target == "" {
			return fmt.Errorf("fabriq: entity %q: edge %+v: field, rel and target are all required", spec.Name, e)
		}
		if !binding.HasColumn(e.Field) {
			return fmt.Errorf("fabriq: entity %q: edge field %q is not a column of %s", spec.Name, e.Field, binding.Table)
		}
	}
	for _, f := range spec.Search.Fields {
		if !binding.HasColumn(f) {
			return fmt.Errorf("fabriq: entity %q: search field %q is not a column of %s", spec.Name, f, binding.Table)
		}
	}
	if spec.Search.Index == "" && len(spec.Search.Fields) > 0 {
		return fmt.Errorf("fabriq: entity %q: search fields declared but index name is empty", spec.Name)
	}
	for _, s := range spec.Subscribe {
		if s.Name == "" {
			return fmt.Errorf("fabriq: entity %q: subscription scope with empty name", spec.Name)
		}
		if s.Field != "" && !binding.HasColumn(s.Field) {
			return fmt.Errorf("fabriq: entity %q: scope %q field %q is not a column of %s", spec.Name, s.Name, s.Field, binding.Table)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.entities[spec.Name]; dup {
		return fmt.Errorf("fabriq: entity %q registered twice", spec.Name)
	}
	if prev, dup := r.byModel[binding.ModelType()]; dup {
		return fmt.Errorf("fabriq: model type %s already bound to entity %q", binding.ModelType(), prev.Spec.Name)
	}
	ent := &Entity{Spec: spec, Binding: binding}
	r.entities[spec.Name] = ent
	r.byModel[binding.ModelType()] = ent
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
	}
	return nil
}
