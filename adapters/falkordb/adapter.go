// Package falkordb is fabriq's graph adapter (PHASE 4 — SCAFFOLD).
//
// FalkorDB speaks RESP, so the adapter rides go-redis (GRAPH.QUERY /
// GRAPH.RO_QUERY); the Cypher dialect lives exclusively in mutate.go and
// is restricted to the openCypher common subset gated by
// adapters/graphtest.
//
// IMPLEMENTED NOW (pure, unit-tested): mutation -> Cypher translation with
// version gating, identifier validation, tenant -> graph routing.
//
// REMAINING FOR PHASE 4 (see TODOs): result-set decoding for Query (the
// FalkorDB reply is a nested array of header+rows), TraverseAndHydrate
// wiring through query.TraverseAndHydrate, ApplyMutations execution, the
// graphtest conformance run against a falkordb/falkordb container, the
// projection consumer wiring, and blue-green rebuild GRAPH.COPY/DELETE.
package falkordb

import (
	"context"
	"fmt"

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

// Adapter implements query.GraphQuerier against FalkorDB.
type Adapter struct {
	client *redis.Client
	reg    *registry.Registry
	rel    query.RelationalQuerier // hydration source for TraverseAndHydrate
}

var _ query.GraphQuerier = (*Adapter)(nil)

// Open dials FalkorDB.
func Open(ctx context.Context, cfg Config, reg *registry.Registry, rel query.RelationalQuerier) (*Adapter, error) {
	client := redis.NewClient(&redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("fabriq: falkordb ping: %w", err)
	}
	return &Adapter{client: client, reg: reg, rel: rel}, nil
}

// Close releases the client.
func (a *Adapter) Close() error { return a.client.Close() }

// Query implements query.GraphQuerier.
//
// TODO(phase 4): execute GRAPH.RO_QUERY against graphForTenant(ctx tenant)
// and decode the FalkorDB result set (header + typed rows) into *[]string
// for single-column traversals and struct slices for multi-column rows.
func (a *Adapter) Query(ctx context.Context, cypher string, params map[string]any, into any) error {
	if _, err := tenant.Require(ctx); err != nil {
		return err
	}
	_ = cypher
	_ = params
	_ = into
	return fmt.Errorf("fabriq: falkordb Query not implemented yet (phase 4)")
}

// TraverseAndHydrate implements query.GraphQuerier.
//
// TODO(phase 4): delegate to query.TraverseAndHydrate(ctx, a.reg, a,
// a.rel, ...) once Query decodes id traversals.
func (a *Adapter) TraverseAndHydrate(ctx context.Context, cypher string, params map[string]any, into any) error {
	return query.TraverseAndHydrate(ctx, a.reg, a, a.rel, cypher, params, into)
}

// ApplyMutations implements query.GraphQuerier.
//
// TODO(phase 4): run cypherFor(m) per mutation via GRAPH.QUERY against the
// target graph, batched per event, with the projection consumer acking
// only after all mutations of an event applied.
func (a *Adapter) ApplyMutations(_ context.Context, target string, muts []projection.Mutation) error {
	for _, m := range muts {
		if _, _, err := cypherFor(m); err != nil {
			return err
		}
	}
	_ = target
	return fmt.Errorf("fabriq: falkordb ApplyMutations not implemented yet (phase 4)")
}
