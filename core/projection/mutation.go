// Package projection turns domain events into engine-neutral mutations and
// (in later phases) drives the consumer loops that apply them.
//
// Appliers are pure: apply(Event) -> []Mutation. Mutations carry no dialect
// — no Cypher, no ES DSL. Adapters translate them (FalkorDB -> MERGE,
// Elasticsearch -> bulk ops) and gate on Version for idempotency: a
// mutation is skipped when the stored aggregate version is >= the
// mutation's version.
package projection

// Mutation is the closed set of engine-neutral projection operations.
type Mutation interface{ isMutation() }

// NodeUpsert creates or updates a graph node. Props are column-keyed and
// always include "version"; adapters use Version for idempotency gating.
type NodeUpsert struct {
	Label   string
	ID      string
	Props   map[string]any
	Version int64
}

// EdgeUpsert creates or refreshes a relationship between two nodes.
type EdgeUpsert struct {
	Rel       string
	FromLabel string
	FromID    string
	ToLabel   string
	ToID      string
	Version   int64
}

// NodeDelete removes a node and, by contract, everything attached to it
// (graph adapters implement detach-delete semantics).
type NodeDelete struct {
	Label string
	ID    string
}

// EdgeDelete removes one outgoing relationship of the given type from a
// node, regardless of target. Version carries the originating event's
// version so adapters can gate stale replays (an old EdgeDelete must not
// remove an edge a newer event created).
type EdgeDelete struct {
	Rel       string
	FromLabel string
	FromID    string
	Version   int64
}

// DocIndex indexes a document into the search projection. Index is the
// logical base name; adapters derive the tenant-routed target. Version
// feeds external version gating.
type DocIndex struct {
	Index   string
	ID      string
	Doc     map[string]any
	Version int64
}

// DocDeindex removes a document from the search projection.
type DocDeindex struct {
	Index string
	ID    string
}

func (NodeUpsert) isMutation() {}
func (EdgeUpsert) isMutation() {}
func (NodeDelete) isMutation() {}
func (EdgeDelete) isMutation() {}
func (DocIndex) isMutation()   {}
func (DocDeindex) isMutation() {}
