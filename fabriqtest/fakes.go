// Package fabriqtest is fabriq's exported test kit: in-memory fakes for
// every port, a combined World wiring them over shared memory, and (in
// later phases) the testcontainers harness and seeded fixtures.
//
// Downstream services unit-test against these fakes; the same behavioral
// contracts are enforced on real adapters by the integration suites.
package fabriqtest

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// ErrFakeNotFound is the not-found error returned by fakes; it is the
// canonical fabriq ErrNotFound so errors.Is works either way.
var ErrFakeNotFound = fabriqerr.ErrNotFound

// World wires all fakes over one shared in-memory store, so a command
// executed against Store is immediately visible through Rel, and graph /
// search fakes can hydrate from the same rows.
type World struct {
	Registry    *registry.Registry
	Store       *FakeStore
	Rel         *FakeRelational
	Graph       *FakeGraph
	Search      *FakeSearch
	TS          *FakeTS
	Vector      *FakeVector
	Docs        *FakeDocumentStore
	Projections *FakeProjectionState
}

// NewWorld builds the linked fake set for a registry.
func NewWorld(reg *registry.Registry) *World {
	db := &memdb{rows: map[string]map[string]map[string]memRow{}}
	rel := &FakeRelational{reg: reg, db: db}
	return &World{
		Registry:    reg,
		Store:       &FakeStore{db: db},
		Rel:         rel,
		Graph:       NewFakeGraph(reg, rel),
		Search:      NewFakeSearch(reg),
		TS:          &FakeTS{data: map[string]map[string]map[string][]query.Point{}},
		Vector:      &FakeVector{data: map[string]map[string]map[string]vecEntry{}},
		Docs:        &FakeDocumentStore{},
		Projections: &FakeProjectionState{applied: map[string]int64{}},
	}
}

// Executor returns a command executor wired to the world's store.
func (w *World) Executor() *command.Executor {
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		panic(err) // registry and store are never nil here
	}
	return x
}

// --- shared memory ---------------------------------------------------------

type memRow struct {
	vals    map[string]any
	version int64
}

type memdb struct {
	mu   sync.RWMutex
	rows map[string]map[string]map[string]memRow // tenant -> entity -> id -> row
}

func (db *memdb) clone() map[string]map[string]map[string]memRow {
	out := make(map[string]map[string]map[string]memRow, len(db.rows))
	for t, entities := range db.rows {
		out[t] = make(map[string]map[string]memRow, len(entities))
		for e, rows := range entities {
			out[t][e] = make(map[string]memRow, len(rows))
			for id, r := range rows {
				out[t][e][id] = r
			}
		}
	}
	return out
}

// --- FakeStore (command.Store) ----------------------------------------------

// FakeStore implements command.Store with real transactional semantics:
// changes stage into a snapshot and merge only on success, so batch
// atomicity behaves like the Postgres adapter.
type FakeStore struct {
	db           *memdb
	mu           sync.Mutex
	outbox       []event.Envelope
	failOnOutbox func() error
}

// Outbox returns every envelope committed so far, in order.
func (s *FakeStore) Outbox() []event.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]event.Envelope, len(s.outbox))
	copy(out, s.outbox)
	return out
}

// FailOnOutbox injects an outbox failure (nil to clear).
func (s *FakeStore) FailOnOutbox(fn func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failOnOutbox = fn
}

// InTenantTx implements command.Store. Fakes run transactions one at a
// time; concurrency tests belong to the real adapter's integration suite.
func (s *FakeStore) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.db.mu.RLock()
	stage := s.db.clone()
	s.db.mu.RUnlock()

	tx := &fakeTx{store: s, tenantID: tid, stage: stage}
	if err := fn(ctx, tx); err != nil {
		return err // staged copy dropped = rollback
	}

	s.db.mu.Lock()
	s.db.rows = stage
	s.db.mu.Unlock()
	s.outbox = append(s.outbox, tx.outbox...)
	return nil
}

type fakeTx struct {
	store    *FakeStore
	tenantID string
	stage    map[string]map[string]map[string]memRow
	outbox   []event.Envelope
}

func (t *fakeTx) CurrentVersion(_ context.Context, ent *registry.Entity, aggID string) (int64, error) {
	return t.stage[t.tenantID][ent.Spec.Name][aggID].version, nil
}

func (t *fakeTx) ApplyChange(_ context.Context, ent *registry.Entity, op command.Op, aggID string, version int64, vals map[string]any) error {
	entities, ok := t.stage[t.tenantID]
	if !ok {
		entities = map[string]map[string]memRow{}
		t.stage[t.tenantID] = entities
	}
	rows, ok := entities[ent.Spec.Name]
	if !ok {
		rows = map[string]memRow{}
		entities[ent.Spec.Name] = rows
	}
	if op == command.OpDelete {
		delete(rows, aggID)
		return nil
	}
	rows[aggID] = memRow{vals: vals, version: version}
	return nil
}

func (t *fakeTx) AppendOutbox(_ context.Context, env event.Envelope) error {
	if t.store.failOnOutbox != nil {
		if err := t.store.failOnOutbox(); err != nil {
			return err
		}
	}
	t.outbox = append(t.outbox, env)
	return nil
}

// Exec implements command.Tx. The in-memory fake has no SQL engine, so raw
// transactional statements (the LifecycleHook participation escape hatch) are
// not supported here — they belong to the postgres adapter integration suite.
func (t *fakeTx) Exec(_ context.Context, _ string, _ ...any) error {
	return fmt.Errorf("fabriq: FakeStore.Tx does not execute raw SQL; use the postgres adapter integration harness")
}

// --- FakeRelational (query.RelationalQuerier) --------------------------------

// FakeRelational reads the world's shared memory, tenant-scoped from ctx.
type FakeRelational struct {
	reg *registry.Registry
	db  *memdb
}

func (r *FakeRelational) entity(name string) (*registry.Entity, error) {
	ent, ok := r.reg.Get(name)
	if !ok {
		return nil, fmt.Errorf("fabriq: unknown entity %q", name)
	}
	return ent, nil
}

// Get implements query.RelationalQuerier.
func (r *FakeRelational) Get(ctx context.Context, entity, id string, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ent, err := r.entity(entity)
	if err != nil {
		return err
	}
	r.db.mu.RLock()
	row, ok := r.db.rows[tid][entity][id]
	r.db.mu.RUnlock()
	if !ok {
		return &fabriqerr.NotFoundError{Entity: entity, ID: id}
	}
	return ent.Binding.Populate(into, row.vals)
}

// GetMany implements the batched hydration contract: one logical lookup,
// results in ids order, missing ids skipped.
func (r *FakeRelational) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ent, err := r.entity(entity)
	if err != nil {
		return err
	}
	slice, elemIsPtr, elemType, err := sliceTarget(into, ent)
	if err != nil {
		return err
	}
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	for _, id := range ids {
		row, ok := r.db.rows[tid][entity][id]
		if !ok {
			continue
		}
		if err := appendRow(slice, elemIsPtr, elemType, ent, row.vals); err != nil {
			return err
		}
	}
	return nil
}

// List implements equality-filtered paging.
func (r *FakeRelational) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ent, err := r.entity(entity)
	if err != nil {
		return err
	}
	slice, elemIsPtr, elemType, err := sliceTarget(into, ent)
	if err != nil {
		return err
	}

	if err := query.ValidateConds(q.Where, ent.Binding.HasColumn); err != nil {
		return err
	}

	r.db.mu.RLock()
	rows := r.db.rows[tid][entity]
	ids := make([]string, 0, len(rows))
	for id, row := range rows {
		ok, evErr := evalConds(row.vals, q.Where)
		if evErr != nil {
			r.db.mu.RUnlock()
			return evErr
		}
		if ok {
			ids = append(ids, id)
		}
	}
	// Order by the requested column (ties and the default break by id) so
	// the fake mirrors the adapter's deterministic ordering.
	orderCol, desc := parseOrderBy(q.OrderBy)
	if orderCol != "" && !ent.Binding.HasColumn(orderCol) {
		r.db.mu.RUnlock()
		return fmt.Errorf("fabriq: entity %q has no order column %q", entity, orderCol)
	}
	sort.SliceStable(ids, func(i, j int) bool {
		if orderCol != "" {
			cmp, ok := compareVals(rows[ids[i]].vals[orderCol], rows[ids[j]].vals[orderCol])
			if ok && cmp != 0 {
				if desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return ids[i] < ids[j]
	})
	r.db.mu.RUnlock()

	if q.Offset > 0 {
		if q.Offset >= len(ids) {
			return nil
		}
		ids = ids[q.Offset:]
	}
	if q.Limit > 0 && len(ids) > q.Limit {
		ids = ids[:q.Limit]
	}
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	for _, id := range ids {
		if err := appendRow(slice, elemIsPtr, elemType, ent, r.db.rows[tid][entity][id].vals); err != nil {
			return err
		}
	}
	return nil
}

// Query is unsupported in the fake — raw SQL belongs to integration tests
// against the real adapter.
func (r *FakeRelational) Query(context.Context, any, string, ...any) error {
	return fmt.Errorf("fabriq: FakeRelational does not execute raw SQL; use the postgres adapter integration harness")
}

func sliceTarget(into any, ent *registry.Entity) (reflect.Value, bool, reflect.Type, error) {
	v := reflect.ValueOf(into)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Slice {
		return reflect.Value{}, false, nil, fmt.Errorf("fabriq: target must be a pointer to slice, got %T", into)
	}
	elem := v.Elem().Type().Elem()
	isPtr := elem.Kind() == reflect.Pointer
	t := elem
	if isPtr {
		t = elem.Elem()
	}
	if t != ent.Binding.ModelType() {
		return reflect.Value{}, false, nil, fmt.Errorf("fabriq: slice element %s does not match entity %q model %s",
			elem, ent.Spec.Name, ent.Binding.ModelType())
	}
	return v.Elem(), isPtr, t, nil
}

func appendRow(slice reflect.Value, elemIsPtr bool, elemType reflect.Type, ent *registry.Entity, vals map[string]any) error {
	m := reflect.New(elemType)
	if err := ent.Binding.Populate(m.Interface(), vals); err != nil {
		return err
	}
	if elemIsPtr {
		slice.Set(reflect.Append(slice, m))
	} else {
		slice.Set(reflect.Append(slice, m.Elem()))
	}
	return nil
}

// --- FakeGraph (query.GraphQuerier) -------------------------------------------

// FakeNode is an inspectable graph node.
type FakeNode struct {
	Props   map[string]any
	Version int64
}

type edgeKey struct {
	rel, fromLabel, fromID, toLabel, toID string
}

type memGraph struct {
	nodes map[string]map[string]FakeNode // label -> id -> node
	edges map[edgeKey]int64              // -> version
}

// FakeGraph applies engine-neutral mutations to an in-memory property
// graph with the same version gating real adapters implement, and serves
// canned responses for raw Cypher (fakes do not parse Cypher).
type FakeGraph struct {
	mu     sync.RWMutex
	reg    *registry.Registry
	rel    query.RelationalQuerier
	graphs map[string]*memGraph
	canned map[string][]string
}

// NewFakeGraph builds a graph fake hydrating through rel.
func NewFakeGraph(reg *registry.Registry, rel query.RelationalQuerier) *FakeGraph {
	return &FakeGraph{reg: reg, rel: rel, graphs: map[string]*memGraph{}, canned: map[string][]string{}}
}

// Cann registers the id list a Cypher string returns (exact match).
func (g *FakeGraph) Cann(cypher string, ids []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.canned[cypher] = ids
}

// Query implements query.GraphQuerier for canned traversals.
func (g *FakeGraph) Query(_ context.Context, cypher string, _ map[string]any, into any) error {
	g.mu.RLock()
	ids, ok := g.canned[cypher]
	g.mu.RUnlock()
	if !ok {
		return fmt.Errorf("fabriq: FakeGraph has no canned response for query %q; register one with Cann", cypher)
	}
	dest, isIDs := into.(*[]string)
	if !isIDs {
		return fmt.Errorf("fabriq: FakeGraph canned queries scan into *[]string, got %T", into)
	}
	*dest = append(*dest, ids...)
	return nil
}

// TraverseAndHydrate composes the canned traversal with one batched
// relational hydration (the no-N+1 contract).
func (g *FakeGraph) TraverseAndHydrate(ctx context.Context, cypher string, params map[string]any, into any) error {
	return query.TraverseAndHydrate(ctx, g.reg, g, g.rel, cypher, params, into)
}

// ApplyMutations implements the projection write path with version gating.
// target "" resolves to the tenant's live graph (tenant from ctx), the
// same contract real sinks implement.
func (g *FakeGraph) ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error {
	if target == "" {
		tid, err := tenant.Require(ctx)
		if err != nil {
			return err
		}
		target = registry.GraphName(tid)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	mg, ok := g.graphs[target]
	if !ok {
		mg = &memGraph{nodes: map[string]map[string]FakeNode{}, edges: map[edgeKey]int64{}}
		g.graphs[target] = mg
	}
	for _, m := range muts {
		switch mut := m.(type) {
		case projection.NodeUpsert:
			byID, ok := mg.nodes[mut.Label]
			if !ok {
				byID = map[string]FakeNode{}
				mg.nodes[mut.Label] = byID
			}
			if cur, exists := byID[mut.ID]; exists && cur.Version >= mut.Version {
				continue // idempotency gate
			}
			props := make(map[string]any, len(mut.Props))
			for k, v := range mut.Props {
				props[k] = v
			}
			byID[mut.ID] = FakeNode{Props: props, Version: mut.Version}
		case projection.EdgeUpsert:
			k := edgeKey{mut.Rel, mut.FromLabel, mut.FromID, mut.ToLabel, mut.ToID}
			if cur, exists := mg.edges[k]; exists && cur >= mut.Version {
				continue
			}
			// An aggregate has at most one outgoing edge per relationship
			// (FK semantics): replace any previous target.
			for old := range mg.edges {
				if old.rel == mut.Rel && old.fromLabel == mut.FromLabel && old.fromID == mut.FromID {
					delete(mg.edges, old)
				}
			}
			mg.edges[k] = mut.Version
		case projection.NodeDelete:
			delete(mg.nodes[mut.Label], mut.ID)
			for k := range mg.edges {
				if (k.fromLabel == mut.Label && k.fromID == mut.ID) || (k.toLabel == mut.Label && k.toID == mut.ID) {
					delete(mg.edges, k)
				}
			}
		case projection.EdgeDelete:
			for k := range mg.edges {
				if k.rel == mut.Rel && k.fromLabel == mut.FromLabel && k.fromID == mut.FromID {
					delete(mg.edges, k)
				}
			}
		case projection.DocIndex, projection.DocDeindex:
			return fmt.Errorf("fabriq: search mutations sent to the graph port")
		default:
			return fmt.Errorf("fabriq: unknown mutation %T", m)
		}
	}
	return nil
}

// Node inspects a node in a target graph.
func (g *FakeGraph) Node(target, label, id string) (FakeNode, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	mg, ok := g.graphs[target]
	if !ok {
		return FakeNode{}, false
	}
	n, ok := mg.nodes[label][id]
	return n, ok
}

// HasEdge inspects an edge in a target graph.
func (g *FakeGraph) HasEdge(target, rel, fromLabel, fromID, toLabel, toID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	mg, ok := g.graphs[target]
	if !ok {
		return false
	}
	_, ok = mg.edges[edgeKey{rel, fromLabel, fromID, toLabel, toID}]
	return ok
}

// --- FakeSearch (query.SearchQuerier) ------------------------------------------

type searchDoc struct {
	doc     map[string]any
	version int64
}

// FakeSearch is a substring-matching search fake with version gating and
// ctx-tenant scoping.
type FakeSearch struct {
	mu      sync.RWMutex
	reg     *registry.Registry
	indexes map[string]map[string]searchDoc // base index -> doc id -> doc
}

// NewFakeSearch builds a search fake.
func NewFakeSearch(reg *registry.Registry) *FakeSearch {
	return &FakeSearch{reg: reg, indexes: map[string]map[string]searchDoc{}}
}

// ApplyMutations implements the projection write path.
func (s *FakeSearch) ApplyMutations(_ context.Context, _ string, muts []projection.Mutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range muts {
		switch mut := m.(type) {
		case projection.DocIndex:
			docs, ok := s.indexes[mut.Index]
			if !ok {
				docs = map[string]searchDoc{}
				s.indexes[mut.Index] = docs
			}
			if cur, exists := docs[mut.ID]; exists && cur.version >= mut.Version {
				continue
			}
			doc := make(map[string]any, len(mut.Doc))
			for k, v := range mut.Doc {
				doc[k] = v
			}
			docs[mut.ID] = searchDoc{doc: doc, version: mut.Version}
		case projection.DocDeindex:
			delete(s.indexes[mut.Index], mut.ID)
		default:
			return fmt.Errorf("fabriq: non-search mutation %T sent to the search port", m)
		}
	}
	return nil
}

// Search implements substring match over the entity's declared fields,
// narrowed by the structured Filter, ordered by Sort (id when empty, since
// the fake has no relevance score) and paginated by Offset/Limit. It
// mirrors the ES adapter's neutral contract closely enough for unit tests;
// the integration suite is the source of truth for scoring and analysis.
func (s *FakeSearch) Search(ctx context.Context, q query.SearchQuery, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ent, ok := s.reg.Get(q.Entity)
	if !ok || ent.Spec.Search.Index == "" {
		return fmt.Errorf("fabriq: entity %q is not searchable", q.Entity)
	}
	dest, ok := into.(*[]map[string]any)
	if !ok {
		return fmt.Errorf("fabriq: FakeSearch scans into *[]map[string]any, got %T", into)
	}
	if err := query.ValidateSearchQuery(q, ent.Spec.Search.Fields); err != nil {
		return err
	}
	needle := strings.ToLower(q.Query)

	s.mu.RLock()
	defer s.mu.RUnlock()
	matched := make([]map[string]any, 0)
	for _, d := range s.indexes[ent.Spec.Search.Index] {
		if d.doc[registry.ColumnTenant] != tid {
			continue
		}
		if needle != "" && !matchesText(d.doc, ent.Spec.Search.Fields, needle) {
			continue
		}
		if len(q.Filter) > 0 {
			pass, err := evalConds(d.doc, q.Filter)
			if err != nil {
				return err
			}
			if !pass {
				continue
			}
		}
		matched = append(matched, d.doc)
	}

	sortCol, desc := query.SortField(q.Sort)
	if sortCol == "" {
		sortCol = registry.ColumnID // no score in the fake: stable id order
	}
	sort.SliceStable(matched, func(i, j int) bool {
		cmp, _ := compareVals(matched[i][sortCol], matched[j][sortCol])
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})

	if q.Offset > 0 {
		if q.Offset >= len(matched) {
			return nil
		}
		matched = matched[q.Offset:]
	}
	if q.Limit > 0 && len(matched) > q.Limit {
		matched = matched[:q.Limit]
	}
	*dest = append(*dest, matched...)
	return nil
}

// matchesText reports whether any declared field contains the needle.
func matchesText(doc map[string]any, fields []string, needle string) bool {
	for _, f := range fields {
		if sv, isStr := doc[f].(string); isStr && strings.Contains(strings.ToLower(sv), needle) {
			return true
		}
	}
	return false
}

// --- FakeTS (query.TSQuerier) ----------------------------------------------------

// FakeTS stores points per (tenant, series, key), time-sorted.
type FakeTS struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string][]query.Point
}

// BulkWrite implements the event-bypass telemetry ingest.
func (f *FakeTS) BulkWrite(ctx context.Context, series string, points []query.Point) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	byseries, ok := f.data[tid]
	if !ok {
		byseries = map[string]map[string][]query.Point{}
		f.data[tid] = byseries
	}
	bykey, ok := byseries[series]
	if !ok {
		bykey = map[string][]query.Point{}
		byseries[series] = bykey
	}
	for _, p := range points {
		bykey[p.Key] = append(bykey[p.Key], p)
	}
	for k := range bykey {
		pts := bykey[k]
		sort.Slice(pts, func(i, j int) bool { return pts[i].At.Before(pts[j].At) })
	}
	return nil
}

// Range implements raw-point reads over [From, To).
func (f *FakeTS) Range(ctx context.Context, q query.RangeQuery, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	dest, ok := into.(*[]query.Point)
	if !ok {
		return fmt.Errorf("fabriq: FakeTS scans into *[]query.Point, got %T", into)
	}
	if q.Bucket > 0 {
		return fmt.Errorf("fabriq: FakeTS does not bucket; aggregate queries belong to the timescale adapter")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, p := range f.data[tid][q.Series][q.Key] {
		if !p.At.Before(q.From) && p.At.Before(q.To) {
			*dest = append(*dest, p)
		}
	}
	return nil
}

// --- FakeVector (query.VectorQuerier) ----------------------------------------------

type vecEntry struct {
	emb  []float32
	meta map[string]any
}

// FakeVector is an exact cosine-similarity store.
type FakeVector struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string]vecEntry // tenant -> entity -> id
}

// Upsert implements query.VectorQuerier.
func (f *FakeVector) Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	byEntity, ok := f.data[tid]
	if !ok {
		byEntity = map[string]map[string]vecEntry{}
		f.data[tid] = byEntity
	}
	byID, ok := byEntity[entity]
	if !ok {
		byID = map[string]vecEntry{}
		byEntity[entity] = byID
	}
	emb := make([]float32, len(embedding))
	copy(emb, embedding)
	byID[id] = vecEntry{emb: emb, meta: meta}
	return nil
}

// Similar implements exact top-K cosine search.
func (f *FakeVector) Similar(ctx context.Context, q query.VectorQuery, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	dest, ok := into.(*[]query.VectorMatch)
	if !ok {
		return fmt.Errorf("fabriq: FakeVector scans into *[]query.VectorMatch, got %T", into)
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	matches := make([]query.VectorMatch, 0)
	for id, e := range f.data[tid][q.Entity] {
		matches = append(matches, query.VectorMatch{ID: id, Score: cosine(q.Embedding, e.emb), Meta: e.meta})
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		return matches[i].ID < matches[j].ID
	})
	if q.K > 0 && len(matches) > q.K {
		matches = matches[:q.K]
	}
	*dest = append(*dest, matches...)
	return nil
}

func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return -1
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return -1
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// --- FakeProjectionState (projection.StateReader) ------------------------------------

// FakeProjectionState tracks applied versions per aggregate for
// WaitForProjection tests; projection consumers (or tests) advance it with
// SetApplied.
type FakeProjectionState struct {
	mu      sync.RWMutex
	applied map[string]int64 // tenant|projection|aggregate|aggID -> version
}

func stateKey(tenantID, proj, aggregate, aggID string) string {
	return tenantID + "|" + proj + "|" + aggregate + "|" + aggID
}

// SetApplied records that a projection has applied an aggregate version
// (implements projection.AppliedRecorder; the watermark never regresses).
func (f *FakeProjectionState) SetApplied(_ context.Context, tenantID, proj, aggregate, aggID string, version int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if version > f.applied[stateKey(tenantID, proj, aggregate, aggID)] {
		f.applied[stateKey(tenantID, proj, aggregate, aggID)] = version
	}
	return nil
}

// AppliedVersion implements projection.StateReader.
func (f *FakeProjectionState) AppliedVersion(_ context.Context, tenantID, proj, aggregate, aggID string) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.applied[stateKey(tenantID, proj, aggregate, aggID)], nil
}

// --- FakeDocumentStore (document.Store) ----------------------------------------------

// FakeDocumentStore is the deferred document plane: every method states
// the plane is not implemented yet (phase 7).
type FakeDocumentStore struct{}

func (FakeDocumentStore) errDeferred() error {
	return fmt.Errorf("fabriq: document plane not implemented yet (phase 7 scaffold): %w", fabriqerr.ErrStoreNotConfigured)
}

// ApplyUpdate implements document.Store (deferred).
func (f *FakeDocumentStore) ApplyUpdate(context.Context, string, []byte) error {
	return f.errDeferred()
}

// Sync implements document.Store (deferred).
func (f *FakeDocumentStore) Sync(context.Context, string, []byte) ([]byte, error) {
	return nil, f.errDeferred()
}

// Snapshot implements document.Store (deferred).
func (f *FakeDocumentStore) Snapshot(context.Context, string) (document.Materialized, error) {
	return document.Materialized{}, f.errDeferred()
}

// Compact implements document.Store (deferred).
func (f *FakeDocumentStore) Compact(context.Context, string) error { return f.errDeferred() }
