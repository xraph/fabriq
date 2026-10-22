package fabriq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
)

// DynamicSpecStore persists dynamic entity descriptors so runtime-defined
// types survive a restart. *adapters/postgres.Adapter satisfies it, backed by
// the fabriq_dynamic_entity table.
type DynamicSpecStore interface {
	SaveDynamicSpec(ctx context.Context, name string, spec []byte) (int64, error)
	DeleteDynamicSpec(ctx context.Context, name string) error
	LoadDynamicSpecs(ctx context.Context) ([]postgres.DynamicSpecRecord, error)
}

// DynamicSyncResult reports what one pass over the persisted dynamic entity
// descriptors changed in the registry. Names are sorted.
type DynamicSyncResult struct {
	// Added are persisted types this facade had not registered yet.
	Added []string
	// Updated are types this facade loaded or persisted whose stored
	// revision moved since.
	Updated []string
	// Removed are types this facade loaded or persisted whose row is gone.
	// They leave the registry; their tables are never dropped by a sync.
	Removed []string
	// Skipped maps a persisted name the pass left alone to the reason.
	Skipped map[string]string
}

// dynamicSpecFormat is the version of the persisted JSON layout. Decoding
// rejects any other value, so a row written by a newer fabriq is skipped
// rather than misread.
const dynamicSpecFormat = 1

// The persisted shapes below are the on-disk contract (format 1). Every
// slice is emitted as [] rather than null so equal specs encode to equal
// bytes whether the caller wrote nil or an empty slice.
type persistedDynamicSpec struct {
	Format      int                 `json:"format"`
	Name        string              `json:"name"`
	Table       string              `json:"table"`
	Columns     []persistedColumn   `json:"columns"`
	Indexes     []persistedIndex    `json:"indexes"`
	NoTypeCheck bool                `json:"noTypeCheck"`
	GraphNode   string              `json:"graphNode"`
	GraphEdge   *persistedGraphEdge `json:"graphEdge"`
	Edges       []persistedEdge     `json:"edges"`
	Search      persistedSearch     `json:"search"`
	Subscribe   []persistedScope    `json:"subscribe"`
	Live        *persistedLiveSpec  `json:"live"`
	Cache       *persistedCacheSpec `json:"cache"`
}

type persistedColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	NotNull bool   `json:"notNull"`
	Default string `json:"default"`
}

type persistedIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

type persistedGraphEdge struct {
	TypeField   string   `json:"typeField"`
	SourceField string   `json:"sourceField"`
	TargetField string   `json:"targetField"`
	SourceLabel string   `json:"sourceLabel"`
	TargetLabel string   `json:"targetLabel"`
	PropFields  []string `json:"propFields"`
}

type persistedEdge struct {
	Field  string `json:"field"`
	Rel    string `json:"rel"`
	Target string `json:"target"`
}

type persistedSearch struct {
	Index  string   `json:"index"`
	Fields []string `json:"fields"`
}

type persistedScope struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}

type persistedLiveSpec struct {
	Filterable []string `json:"filterable"`
	Sortable   []string `json:"sortable"`
	MaxWindow  int      `json:"maxWindow"`
}

type persistedCacheSpec struct {
	TTLSeconds int64 `json:"ttlSeconds"`
	Scoped     bool  `json:"scoped"`
}

var columnTypeNames = map[registry.ColumnType]string{
	registry.ColText:  "text",
	registry.ColInt:   "int",
	registry.ColFloat: "float",
	registry.ColBool:  "bool",
	registry.ColTime:  "time",
	registry.ColJSON:  "json",
}

func columnTypeFromName(s string) (registry.ColumnType, bool) {
	for t, n := range columnTypeNames {
		if n == s {
			return t, true
		}
	}
	return 0, false
}

// strs returns s, or an empty non-nil slice, so it encodes as [].
func strs(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// strsOrNil is the decode-side inverse of strs.
func strsOrNil(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}

// encodeDynamicSpec renders the declarative part of a dynamic spec as format 1
// JSON. Validate, Embed, Distill, Analytics, Insights, Metrics and CRDT are
// left out: they hold functions or in-process wiring and never persist.
func encodeDynamicSpec(spec registry.EntitySpec) ([]byte, error) {
	if spec.Schema == nil {
		return nil, fmt.Errorf("fabriq: entity %q is not a dynamic entity (Schema is nil)", spec.Name)
	}
	p := persistedDynamicSpec{
		Format:      dynamicSpecFormat,
		Name:        spec.Name,
		Table:       spec.Schema.Table,
		Columns:     make([]persistedColumn, 0, len(spec.Schema.Columns)),
		Indexes:     make([]persistedIndex, 0, len(spec.Schema.Indexes)),
		NoTypeCheck: spec.Schema.NoTypeCheck,
		GraphNode:   spec.GraphNode,
		Edges:       make([]persistedEdge, 0, len(spec.Edges)),
		Search:      persistedSearch{Index: spec.Search.Index, Fields: strs(spec.Search.Fields)},
		Subscribe:   make([]persistedScope, 0, len(spec.Subscribe)),
	}
	for _, c := range spec.Schema.Columns {
		tn, ok := columnTypeNames[c.Type]
		if !ok {
			return nil, fmt.Errorf("fabriq: entity %q: column %q has unknown type %d", spec.Name, c.Name, c.Type)
		}
		p.Columns = append(p.Columns, persistedColumn{Name: c.Name, Type: tn, NotNull: c.NotNull, Default: c.Default})
	}
	for _, ix := range spec.Schema.Indexes {
		p.Indexes = append(p.Indexes, persistedIndex{Name: ix.Name, Columns: strs(ix.Columns), Unique: ix.Unique})
	}
	if ge := spec.GraphEdge; ge != nil {
		p.GraphEdge = &persistedGraphEdge{
			TypeField: ge.TypeField, SourceField: ge.SourceField, TargetField: ge.TargetField,
			SourceLabel: ge.SourceLabel, TargetLabel: ge.TargetLabel, PropFields: strs(ge.PropFields),
		}
	}
	for _, e := range spec.Edges {
		p.Edges = append(p.Edges, persistedEdge{Field: e.Field, Rel: e.Rel, Target: e.Target})
	}
	for _, s := range spec.Subscribe {
		p.Subscribe = append(p.Subscribe, persistedScope{Name: s.Name, Field: s.Field})
	}
	if l := spec.Live; l != nil {
		p.Live = &persistedLiveSpec{Filterable: strs(l.Filterable), Sortable: strs(l.Sortable), MaxWindow: l.MaxWindow}
	}
	if c := spec.Cache; c != nil {
		p.Cache = &persistedCacheSpec{TTLSeconds: int64(c.TTL / time.Second), Scoped: c.Scoped}
	}
	return json.Marshal(p)
}

// decodeDynamicSpec parses format 1 JSON back into a KindAggregate dynamic
// spec. Empty lists come back as nil slices.
func decodeDynamicSpec(b []byte) (registry.EntitySpec, error) {
	var p persistedDynamicSpec
	if err := json.Unmarshal(b, &p); err != nil {
		return registry.EntitySpec{}, err
	}
	if p.Format != dynamicSpecFormat {
		return registry.EntitySpec{}, fmt.Errorf("unsupported dynamic spec format %d", p.Format)
	}
	if p.Name == "" || p.Table == "" {
		return registry.EntitySpec{}, fmt.Errorf("dynamic spec needs a name and a table")
	}
	sc := &registry.DynamicSchema{Table: p.Table, NoTypeCheck: p.NoTypeCheck}
	for _, c := range p.Columns {
		t, ok := columnTypeFromName(c.Type)
		if !ok {
			return registry.EntitySpec{}, fmt.Errorf("column %q has unknown type %q", c.Name, c.Type)
		}
		sc.Columns = append(sc.Columns, registry.DynamicColumn{Name: c.Name, Type: t, NotNull: c.NotNull, Default: c.Default})
	}
	for _, ix := range p.Indexes {
		sc.Indexes = append(sc.Indexes, registry.DynamicIndex{Name: ix.Name, Columns: strsOrNil(ix.Columns), Unique: ix.Unique})
	}
	spec := registry.EntitySpec{
		Name:      p.Name,
		Kind:      registry.KindAggregate,
		Schema:    sc,
		GraphNode: p.GraphNode,
		Search:    registry.SearchSpec{Index: p.Search.Index, Fields: strsOrNil(p.Search.Fields)},
	}
	if ge := p.GraphEdge; ge != nil {
		spec.GraphEdge = &registry.GraphEdgeSpec{
			TypeField: ge.TypeField, SourceField: ge.SourceField, TargetField: ge.TargetField,
			SourceLabel: ge.SourceLabel, TargetLabel: ge.TargetLabel, PropFields: strsOrNil(ge.PropFields),
		}
	}
	for _, e := range p.Edges {
		spec.Edges = append(spec.Edges, registry.EdgeSpec{Field: e.Field, Rel: e.Rel, Target: e.Target})
	}
	for _, s := range p.Subscribe {
		spec.Subscribe = append(spec.Subscribe, registry.Scope{Name: s.Name, Field: s.Field})
	}
	if l := p.Live; l != nil {
		spec.Live = &registry.LiveSpec{Filterable: strsOrNil(l.Filterable), Sortable: strsOrNil(l.Sortable), MaxWindow: l.MaxWindow}
	}
	if c := p.Cache; c != nil {
		spec.Cache = &registry.CacheSpec{TTL: time.Duration(c.TTLSeconds) * time.Second, Scoped: c.Scoped}
	}
	return spec, nil
}

// sameDynamicEncoding reports whether two specs persist to identical bytes,
// meaning they differ at most in in-process-only fields.
func sameDynamicEncoding(a, b registry.EntitySpec) bool {
	ea, err := encodeDynamicSpec(a)
	if err != nil {
		return false
	}
	eb, err := encodeDynamicSpec(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ea, eb)
}

// withInProcessFields copies the fields a descriptor never persists from src
// onto dst, so a sync that replaces a type keeps what the app attached.
func withInProcessFields(dst, src registry.EntitySpec) registry.EntitySpec {
	dst.Validate = src.Validate
	dst.Embed = src.Embed
	dst.Distill = src.Distill
	dst.Analytics = src.Analytics
	dst.Insights = src.Insights
	dst.Metrics = src.Metrics
	dst.CRDT = src.CRDT
	return dst
}

func persistError(name string, err error) error {
	return fmt.Errorf("fabriq: persist dynamic entity %q: %w", name, err)
}

// saveDynamicSpec writes b for name and records the revision in loaded.
func saveDynamicSpec(ctx context.Context, store DynamicSpecStore, loaded map[string]int64, name string, b []byte) error {
	rev, err := store.SaveDynamicSpec(ctx, name, b)
	if err != nil {
		return persistError(name, err)
	}
	if loaded != nil {
		loaded[name] = rev
	}
	return nil
}

// syncPersistedDynamic reconciles the registry with the persisted
// descriptors. It only ever touches names this facade loaded or persisted
// (the keys of loaded) plus names nothing has registered yet; a type
// registered in code is reported in Skipped and left alone. A row that will
// not decode or register, or whose table cannot be ensured, is skipped with
// its reason and retried on the next pass. Only a failure to read the store
// returns an error.
func syncPersistedDynamic(ctx context.Context, reg *registry.Registry, ddl DynamicDDL, store DynamicSpecStore, loaded map[string]int64) (DynamicSyncResult, error) {
	res := DynamicSyncResult{Skipped: map[string]string{}}
	if store == nil {
		return res, nil
	}
	recs, err := store.LoadDynamicSpecs(ctx)
	if err != nil {
		return res, err
	}
	seen := make(map[string]bool, len(recs))
	for _, rec := range recs {
		seen[rec.Name] = true
		spec, derr := decodeDynamicSpec(rec.Spec)
		if derr == nil && spec.Name != rec.Name {
			derr = fmt.Errorf("spec names %q but the row is %q", spec.Name, rec.Name)
		}
		if derr != nil {
			res.Skipped[rec.Name] = "invalid spec: " + derr.Error()
			continue
		}
		known, owned := loaded[rec.Name]
		cur, registered := reg.Get(rec.Name)
		switch {
		case registered && !owned:
			res.Skipped[rec.Name] = "registered in code"
		case !registered:
			if rerr := reg.Register(spec); rerr != nil {
				res.Skipped[rec.Name] = "invalid spec: " + rerr.Error()
				continue
			}
			ent, _ := reg.Get(rec.Name)
			if eerr := ddl.EnsureDynamic(ctx, ent); eerr != nil {
				_ = reg.Unregister(rec.Name)
				delete(loaded, rec.Name)
				res.Skipped[rec.Name] = "ensure table: " + eerr.Error()
				continue
			}
			ddl.AddGuardedTable(spec.Schema.Table)
			loaded[rec.Name] = rec.Revision
			res.Added = append(res.Added, rec.Name)
		case rec.Revision != known || !sameDynamicEncoding(cur.Spec, spec):
			// A different revision, higher or (after a drop and redefine
			// elsewhere) lower, means the row changed under us. So does a
			// different encoding at the same revision: a drop and redefine
			// between two passes restarts the row at revision 1.
			prev := cur.Spec
			next := withInProcessFields(spec, prev)
			if rerr := reg.Replace(next); rerr != nil {
				// The app's in-process fields may name a column the new
				// revision dropped or renamed; fall back to the bare spec.
				next = spec
				if rerr = reg.Replace(next); rerr != nil {
					res.Skipped[rec.Name] = "invalid spec: " + rerr.Error()
					continue
				}
			}
			ent, _ := reg.Get(rec.Name)
			if eerr := ddl.EnsureDynamic(ctx, ent); eerr != nil {
				_ = reg.Replace(prev)
				res.Skipped[rec.Name] = "ensure table: " + eerr.Error()
				continue
			}
			if prev.Schema != nil && prev.Schema.Table != next.Schema.Table {
				ddl.RemoveGuardedTable(prev.Schema.Table)
			}
			ddl.AddGuardedTable(next.Schema.Table)
			loaded[rec.Name] = rec.Revision
			res.Updated = append(res.Updated, rec.Name)
		}
	}
	for name := range loaded {
		if seen[name] {
			continue
		}
		if ent, ok := reg.Get(name); ok && ent.Spec.Schema != nil {
			ddl.RemoveGuardedTable(ent.Spec.Schema.Table)
			_ = reg.Unregister(name)
		}
		delete(loaded, name)
		res.Removed = append(res.Removed, name)
	}
	sort.Strings(res.Added)
	sort.Strings(res.Updated)
	sort.Strings(res.Removed)
	return res, nil
}

// --- facade ---

// dynamicStore returns the descriptor store when persistence is on, else nil.
// It never returns a typed-nil interface.
func (f *Fabriq) dynamicStore() DynamicSpecStore {
	if !f.persistDynamic || f.stores == nil || f.stores.Postgres == nil {
		return nil
	}
	return f.stores.Postgres
}

// dynLoadedMap returns the loaded-revision map, creating it on first use.
// The caller holds dynMu.
func (f *Fabriq) dynLoadedMap() map[string]int64 {
	if f.dynLoaded == nil {
		f.dynLoaded = map[string]int64{}
	}
	return f.dynLoaded
}

// SyncPersistedDynamic reloads the persisted dynamic entity descriptors into
// the registry: new rows register and get their tables ensured, rows whose
// revision moved replace the type, and types this facade loaded whose row is
// gone leave the registry (their tables stay). Types registered in code are
// never touched. Open runs it once when Config.Dynamic.Persist is set; the
// worker runs it on a timer so instances converge. With persistence off it
// does nothing and returns an empty result.
func (f *Fabriq) SyncPersistedDynamic(ctx context.Context) (DynamicSyncResult, error) {
	ddl, err := f.dynamicDDL()
	if err != nil {
		return DynamicSyncResult{}, err
	}
	store := f.dynamicStore()
	if store == nil {
		return DynamicSyncResult{}, nil
	}
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	res, err := syncPersistedDynamic(ctx, f.reg, ddl, store, f.dynLoadedMap())
	if err != nil {
		return res, err
	}
	f.dynLast = res
	return cloneSyncResult(res), nil
}

// LastDynamicSync returns the result of the most recent successful
// SyncPersistedDynamic, including the one Open runs. It is the zero value
// before any sync.
func (f *Fabriq) LastDynamicSync() DynamicSyncResult {
	f.dynMu.Lock()
	defer f.dynMu.Unlock()
	return cloneSyncResult(f.dynLast)
}

func cloneSyncResult(r DynamicSyncResult) DynamicSyncResult {
	out := DynamicSyncResult{
		Added:   append([]string(nil), r.Added...),
		Updated: append([]string(nil), r.Updated...),
		Removed: append([]string(nil), r.Removed...),
	}
	if r.Skipped != nil {
		out.Skipped = make(map[string]string, len(r.Skipped))
		for k, v := range r.Skipped {
			out.Skipped[k] = v
		}
	}
	return out
}
