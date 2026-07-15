package registry

import (
	"fmt"
	"reflect"
	"regexp"
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

	// metrics indexes MetricSpec by name across all registered entities. It is
	// built and validated by Validate; empty (nil-lookup-safe) until then.
	metrics map[string]*MetricSpec

	// tablePrefix namespaces STATIC entity tables (WithTablePrefix) so a
	// host embedding fabriq next to its own tables cannot clash. fabriq_*
	// and ds_* tables are already namespaces and are never re-prefixed.
	tablePrefix string
}

// Option configures a Registry at construction.
type Option func(*Registry)

// tablePrefixPattern is the shape a table prefix must have: lower-snake,
// starting with a letter, ending with the separating underscore.
var tablePrefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*_$`)

// WithTablePrefix namespaces every static entity table registered with
// this registry (e.g. "acme_" turns table "widgets" into "acme_widgets").
// fabriq_* infra tables and ds_* dynamic tables keep their existing
// namespaces. An invalid prefix panics: this is wiring-time
// misconfiguration, not runtime input.
func WithTablePrefix(prefix string) Option {
	if prefix != "" && !tablePrefixPattern.MatchString(prefix) {
		panic(fmt.Sprintf("fabriq: invalid table prefix %q (want lower-snake ending in _)", prefix))
	}
	return func(r *Registry) { r.tablePrefix = prefix }
}

// New returns an empty registry.
func New(opts ...Option) *Registry {
	r := &Registry{
		entities: make(map[string]*Entity),
		byModel:  make(map[reflect.Type]*Entity),
		metrics:  make(map[string]*MetricSpec),
	}
	for _, o := range opts {
		o(r)
	}
	return r
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

	binding, err := bind(spec, r.tablePrefix)
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
	if spec.Analytics != nil && !spec.Analytics.IncludeAll && len(spec.Analytics.Include) == 0 && len(spec.Analytics.Hash) == 0 {
		return nil, fmt.Errorf("fabriq: entity %q: Analytics spec has no Include or Hash fields and IncludeAll is false (nothing would be analyticized)", spec.Name)
	}
	if spec.Insights != nil && len(spec.Insights.Measures) == 0 && len(spec.Insights.Dimensions) == 0 {
		return nil, fmt.Errorf("fabriq: entity %q: Insights spec has no Measures or Dimensions (nothing to project)", spec.Name)
	}
	if spec.Insights != nil {
		for _, c := range spec.Insights.Measures {
			if !binding.HasColumn(c) {
				return nil, fmt.Errorf("fabriq: entity %q: Insights measure %q is not a column of %s", spec.Name, c, binding.Table)
			}
		}
		for _, c := range spec.Insights.Dimensions {
			if !binding.HasColumn(c) {
				return nil, fmt.Errorf("fabriq: entity %q: Insights dimension %q is not a column of %s", spec.Name, c, binding.Table)
			}
		}
	}
	for _, m := range spec.Metrics {
		if m.Name == "" {
			return nil, fmt.Errorf("fabriq: entity %q: MetricSpec has an empty Name", spec.Name)
		}
		if len(m.Measures) == 0 {
			return nil, fmt.Errorf("fabriq: entity %q: metric %q has no Measures", spec.Name, m.Name)
		}
		for _, mm := range m.Measures {
			// count needs no field; every other kind requires a real column.
			if mm.Kind != "count" && !binding.HasColumn(mm.Field) {
				return nil, fmt.Errorf("fabriq: entity %q: metric %q measure field %q is not a column of %s", spec.Name, m.Name, mm.Field, binding.Table)
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

// Metric returns the declared MetricSpec named name. The index is populated by
// Validate; calling Metric before Validate returns (nil, false).
func (r *Registry) Metric(name string) (*MetricSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.metrics[name]
	return m, ok
}

// MaterializedMetrics returns every declared metric that opts into
// materialization (MetricSpec.Rollup != nil), sorted by Name for determinism.
// Like Metric, it reads the index built by Validate; calling it before
// Validate returns an empty slice.
func (r *Registry) MaterializedMetrics() []*MetricSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*MetricSpec, 0, len(r.metrics))
	for _, m := range r.metrics {
		if m.Rollup != nil {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// EntityHasInsights reports whether name is a registered entity carrying a
// non-nil InsightsSpec (i.e. one whose projected facts are queryable).
func (r *Registry) EntityHasInsights(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entities[name]
	return ok && e.Spec.Insights != nil
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
// must itself be registered, plus the metric-name index (global uniqueness,
// no entity-name collision, and every entity source declares InsightsSpec).
// Call once after all Register calls.
//
// Validate takes the WRITE lock, not RLock: it assigns r.metrics as its last
// step, and Validate is expected to run once at startup (no read contention
// to protect), so upgrading to Lock for the whole method is simplest and
// avoids any read/write race on that assignment.
func (r *Registry) Validate() error {
	r.mu.Lock()
	defer r.mu.Unlock()
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

	// Metric-name index: global uniqueness, no entity-name collision, and
	// every entity source declares an InsightsSpec. "Event" sources (a Source
	// that names no registered entity) can't be validated against a fixed set
	// — event names are schemaless — so they're accepted as-is.
	idx := map[string]*MetricSpec{}
	for name, e := range r.entities {
		for i := range e.Spec.Metrics {
			m := &e.Spec.Metrics[i]
			if _, clash := r.entities[m.Name]; clash {
				return fmt.Errorf("fabriq: metric %q (on entity %q) collides with a registered entity name", m.Name, name)
			}
			if _, dup := idx[m.Name]; dup {
				return fmt.Errorf("fabriq: duplicate metric name %q", m.Name)
			}
			if src, ok := r.entities[m.Source]; ok && src.Spec.Insights == nil {
				return fmt.Errorf("fabriq: metric %q sources entity %q which has no InsightsSpec", m.Name, m.Source)
			}
			if m.Rollup != nil {
				if m.Rollup.Bucket <= 0 {
					return fmt.Errorf("fabriq: metric %q: Rollup.Bucket must be > 0", m.Name)
				}
				if _, ok := r.entities[m.Source]; ok {
					return fmt.Errorf("fabriq: metric %q: Rollup sources entity %q — rollups are event-sourced only", m.Name, m.Source)
				}
				// Sketch measures (count_distinct/percentile) are allowed on a
				// Rollup metric as of phase 2b-2 — they materialize as
				// toolkit-typed columns (hyperloglog/tdigest) instead of plain
				// NUMERIC. See adapters/postgres's rollupTableDDL (column
				// shape) and toolkitAvailable (the boot-time capability check
				// that fails loudly when timescaledb_toolkit is absent).
			}
			idx[m.Name] = m
		}
	}
	r.metrics = idx

	return nil
}
