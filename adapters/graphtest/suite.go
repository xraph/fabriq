// Package graphtest is the EXPORTED graph-dialect conformance suite: the
// gate every GraphQuerier must pass before fabriq will project into it,
// and the contract that keeps shipped Cypher inside the openCypher common
// subset so FalkorDB can later be swapped for Memgraph, Neo4j or Kùzu
// without touching appliers or call sites.
//
// Usage (any adapter, typically behind an integration build tag):
//
//	func TestFalkorConformance(t *testing.T) {
//	    graphtest.Run(t, func(t *testing.T) graphtest.Harness {
//	        // boot container, return the adapter + a fresh target graph
//	    })
//	}
package graphtest

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
)

// Harness is what an adapter's test factory provides per case.
type Harness struct {
	Graph  query.GraphQuerier
	Target string          // fresh graph/projection target for this case
	Ctx    context.Context // tenant-stamped context
}

// Case is one conformance scenario: seed mutations, a query in the
// openCypher COMMON SUBSET, and the ids it must return (in order).
type Case struct {
	Name    string
	Seed    []projection.Mutation
	Cypher  string
	Params  map[string]any
	WantIDs []string
}

// Cases is the canonical pattern table. Engines must pass every case
// verbatim — no dialect-specific rewrites.
func Cases() []Case {
	seed := []projection.Mutation{
		projection.NodeUpsert{Label: "Site", ID: "S1", Props: map[string]any{"id": "S1", "name": "Plant", "version": int64(1)}, Version: 1},
		projection.NodeUpsert{Label: "Asset", ID: "A1", Props: map[string]any{"id": "A1", "name": "Pump", "version": int64(1)}, Version: 1},
		projection.NodeUpsert{Label: "Asset", ID: "A2", Props: map[string]any{"id": "A2", "name": "Valve", "version": int64(1)}, Version: 1},
		projection.EdgeUpsert{Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A1", ToLabel: "Site", ToID: "S1", Version: 1},
		projection.EdgeUpsert{Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A2", ToLabel: "Site", ToID: "S1", Version: 1},
		projection.EdgeUpsert{Rel: "CHILD_OF", FromLabel: "Asset", FromID: "A2", ToLabel: "Asset", ToID: "A1", Version: 1},
	}
	return []Case{
		{
			Name:    "match by property",
			Seed:    seed,
			Cypher:  `MATCH (a:Asset {id: $id}) RETURN a.id`,
			Params:  map[string]any{"id": "A1"},
			WantIDs: []string{"A1"},
		},
		{
			Name:    "single hop traversal with ordering",
			Seed:    seed,
			Cypher:  `MATCH (a:Asset)-[:LOCATED_AT]->(s:Site {id: $site}) RETURN a.id ORDER BY a.id`,
			Params:  map[string]any{"site": "S1"},
			WantIDs: []string{"A1", "A2"},
		},
		{
			// Source-anchored forward and reverse hops are the shapes the
			// bounded Repo.Out / Repo.In helpers emit; gate both arrow
			// directions so those helpers stay portable across engines.
			Name:    "source-anchored forward hop",
			Seed:    seed,
			Cypher:  `MATCH (n:Asset {id: $id})-[:CHILD_OF]->(m:Asset) RETURN m.id ORDER BY m.id`,
			Params:  map[string]any{"id": "A2"},
			WantIDs: []string{"A1"},
		},
		{
			Name:    "reverse hop traversal",
			Seed:    seed,
			Cypher:  `MATCH (n:Asset {id: $id})<-[:CHILD_OF]-(m:Asset) RETURN m.id ORDER BY m.id`,
			Params:  map[string]any{"id": "A1"},
			WantIDs: []string{"A2"},
		},
		{
			Name:    "variable length path",
			Seed:    seed,
			Cypher:  `MATCH (a:Asset)-[:CHILD_OF*1..3]->(root:Asset {id: $root}) RETURN a.id`,
			Params:  map[string]any{"root": "A1"},
			WantIDs: []string{"A2"},
		},
		{
			Name:    "where with comparison and order",
			Seed:    seed,
			Cypher:  `MATCH (a:Asset) WHERE a.version >= $v RETURN a.id ORDER BY a.id`,
			Params:  map[string]any{"v": int64(1)},
			WantIDs: []string{"A1", "A2"},
		},
		{
			Name: "delete removes from traversal",
			Seed: append(append([]projection.Mutation{}, seed...),
				projection.NodeDelete{Label: "Asset", ID: "A2"},
			),
			Cypher:  `MATCH (a:Asset)-[:LOCATED_AT]->(:Site {id: $site}) RETURN a.id ORDER BY a.id`,
			Params:  map[string]any{"site": "S1"},
			WantIDs: []string{"A1"},
		},
		{
			Name: "stale version is a no-op",
			Seed: append(append([]projection.Mutation{}, seed...),
				projection.NodeUpsert{Label: "Asset", ID: "A1", Props: map[string]any{"id": "A1", "name": "OLD", "version": int64(0)}, Version: 0},
			),
			Cypher:  `MATCH (a:Asset {id: $id}) WHERE a.name = $name RETURN a.id`,
			Params:  map[string]any{"id": "A1", "name": "Pump"},
			WantIDs: []string{"A1"},
		},
	}
}

// Run executes the suite against a fresh harness per case.
func Run(t *testing.T, factory func(t *testing.T) Harness) {
	t.Helper()
	for _, tc := range Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			h := factory(t)
			if err := h.Graph.ApplyMutations(h.Ctx, h.Target, tc.Seed); err != nil {
				t.Fatalf("seed: %v", err)
			}
			var ids []string
			if err := h.Graph.Query(h.Ctx, tc.Cypher, tc.Params, &ids); err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(ids) != len(tc.WantIDs) {
				t.Fatalf("ids = %v, want %v", ids, tc.WantIDs)
			}
			for i := range ids {
				if ids[i] != tc.WantIDs[i] {
					t.Fatalf("ids = %v, want %v", ids, tc.WantIDs)
				}
			}
		})
	}
}
