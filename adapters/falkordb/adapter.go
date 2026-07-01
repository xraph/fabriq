// Package falkordb is fabriq's graph adapter. FalkorDB speaks RESP, so it
// rides go-redis (GRAPH.QUERY / GRAPH.RO_QUERY); the Cypher dialect lives
// exclusively in mutate.go and stays inside the openCypher common subset
// gated by adapters/graphtest — the engine-swap contract.
//
// Tenancy: reads resolve the tenant's LIVE graph through the injected
// resolver (fabriq.Open wires one over projection_state, so blue-green
// rebuilds flip readers atomically); projection writes receive an
// explicit target ("" = live). Graph names only ever come from
// core/registry derivations.
package falkordb

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// Config locates the FalkorDB instance.
type Config struct {
	Addr     string
	Username string
	Password string
}

// TargetResolver maps a tenant onto its live graph name.
type TargetResolver func(ctx context.Context, tenantID string) (string, error)

// Adapter implements query.GraphQuerier against FalkorDB.
type Adapter struct {
	client     *redis.Client
	reg        *registry.Registry
	rel        query.RelationalQuerier // hydration source for TraverseAndHydrate
	liveTarget TargetResolver
}

var _ query.GraphQuerier = (*Adapter)(nil)

// Option customizes the adapter.
type Option func(*Adapter)

// WithLiveTargetResolver overrides live-graph resolution (production wires
// a projection_state-backed resolver; the default derives tenant_{id}).
func WithLiveTargetResolver(fn TargetResolver) Option {
	return func(a *Adapter) {
		if fn != nil {
			a.liveTarget = fn
		}
	}
}

// Open dials FalkorDB.
func Open(ctx context.Context, cfg Config, reg *registry.Registry, rel query.RelationalQuerier, opts ...Option) (*Adapter, error) {
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("fabriq: falkordb ping: %w", err)
	}
	a := &Adapter{
		client: client,
		reg:    reg,
		rel:    rel,
		liveTarget: func(_ context.Context, tenantID string) (string, error) {
			return registry.GraphName(tenantID), nil
		},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

// Close releases the client.
func (a *Adapter) Close() error { return a.client.Close() }

// Query implements query.GraphQuerier: a read-only openCypher query
// against the tenant's live graph. into may be *[]string (single-column
// traversals) or *[]map[string]any (column-keyed rows).
//
// Scope-aware reads: FalkorDB has no RLS; scope is a node property. This
// adapter operates on raw caller-supplied Cypher, so it CANNOT auto-inject
// a scope predicate. Instead, when a scope is present on ctx, it is injected
// into params as "$scope" so callers can reference it directly in their
// Cypher: WHERE n.scope_id IS NULL OR n.scope_id = $scope. An unscoped
// context omits "scope" from params (absent key = see all within tenant).
// The graph consumer (kgkit) adds the predicate in its generated traversals.
func (a *Adapter) Query(ctx context.Context, cypher string, params map[string]any, into any) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	graph, err := a.liveTarget(ctx, tid)
	if err != nil {
		return err
	}
	params = scopeParams(ctx, params)
	cols, rows, err := a.run(ctx, "GRAPH.RO_QUERY", graph, cypher, params)
	if err != nil {
		return err
	}
	return scanRows(cols, rows, into)
}

// scopeParams returns a copy of params with "scope" injected when the context
// carries a secondary scope. The caller's Cypher can then use $scope as a
// predicate: WHERE n.scope_id IS NULL OR n.scope_id = $scope. When the
// context is unscoped the original params map is returned unchanged.
func scopeParams(ctx context.Context, params map[string]any) map[string]any {
	scope, ok := tenant.ScopeFromContext(ctx)
	if !ok {
		return params
	}
	merged := make(map[string]any, len(params)+1)
	for k, v := range params {
		merged[k] = v
	}
	merged["scope"] = scope
	return merged
}

// TraverseAndHydrate implements query.GraphQuerier: traversal returns ids,
// hydration is ONE batched relational query. Never N+1.
func (a *Adapter) TraverseAndHydrate(ctx context.Context, cypher string, params map[string]any, into any) error {
	if a.rel == nil {
		return fmt.Errorf("fabriq: TraverseAndHydrate needs a relational querier")
	}
	return query.TraverseAndHydrate(ctx, a.reg, a, a.rel, cypher, params, into)
}

// ApplyMutations implements the projection write path: engine-neutral
// mutations onto an explicit target graph ("" = the tenant's live graph,
// resolved from the event's tenant on ctx). Version gating in the dialect
// makes replays idempotent.
func (a *Adapter) ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error {
	if target == "" {
		tid, err := tenant.Require(ctx)
		if err != nil {
			return err
		}
		live, err := a.liveTarget(ctx, tid)
		if err != nil {
			return err
		}
		target = live
	}
	for _, m := range muts {
		cy, params, err := cypherFor(m)
		if err != nil {
			return err
		}
		if _, _, err := a.run(ctx, "GRAPH.QUERY", target, cy, params); err != nil {
			return fmt.Errorf("fabriq: apply %T to %s: %w", m, target, err)
		}
	}
	return nil
}

// DropTarget removes a graph (rebuild old-target cleanup). Dropping a
// graph that never received a write is a no-op.
func (a *Adapter) DropTarget(ctx context.Context, target string) error {
	err := a.client.Do(ctx, "GRAPH.DELETE", target).Err()
	if err == nil || strings.Contains(err.Error(), "empty key") {
		return nil
	}
	return translateGraph("GRAPH.DELETE "+target, err)
}

// run executes one GRAPH.* command and decodes header + rows.
func (a *Adapter) run(ctx context.Context, cmd, graph, cypher string, params map[string]any) (cols []string, rows [][]any, err error) {
	prefix, err := cypherParams(params)
	if err != nil {
		return nil, nil, err
	}
	res, err := a.client.Do(ctx, cmd, graph, prefix+cypher).Result()
	if err != nil {
		return nil, nil, translateGraph(cmd+" "+graph, err)
	}
	top, ok := res.([]any)
	if !ok || len(top) < 3 {
		// Write-only result sets ([stats]) have no rows.
		return nil, nil, nil
	}
	header, _ := top[0].([]any)
	cols = make([]string, 0, len(header))
	for _, h := range header {
		switch v := h.(type) {
		case string:
			cols = append(cols, v)
		case []any:
			// Compact-mode header entry: [type, name].
			if len(v) > 0 {
				if name, ok := v[len(v)-1].(string); ok {
					cols = append(cols, name)
				}
			}
		}
	}
	rawRows, _ := top[1].([]any)
	rows = make([][]any, 0, len(rawRows))
	for _, rr := range rawRows {
		cells, ok := rr.([]any)
		if !ok {
			continue
		}
		rows = append(rows, cells)
	}
	return cols, rows, nil
}

// scanRows maps decoded rows into the supported targets.
func scanRows(cols []string, rows [][]any, into any) error {
	switch dest := into.(type) {
	case *[]string:
		if len(rows) > 0 && len(cols) != 1 {
			return fmt.Errorf("fabriq: scanning %d columns into *[]string; single-column traversals only", len(cols))
		}
		for _, row := range rows {
			if len(row) == 0 || row[0] == nil {
				continue
			}
			*dest = append(*dest, cellString(row[0]))
		}
		return nil
	case *[]map[string]any:
		for _, row := range rows {
			m := make(map[string]any, len(cols))
			for i, col := range cols {
				if i < len(row) {
					m[col] = row[i]
				}
			}
			*dest = append(*dest, m)
		}
		return nil
	default:
		return fmt.Errorf("fabriq: graph queries scan into *[]string or *[]map[string]any, got %T", into)
	}
}

func cellString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
