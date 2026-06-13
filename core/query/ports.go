// Package query defines fabriq's capability ports — explicit, engine-typed
// interfaces per storage capability — and the Fabric facade that exposes
// them. There is deliberately no unified query language: relational work
// speaks SQL through grove, graph work speaks openCypher, search speaks
// queries against declared fields. No engine types (pgx, grove, Falkor,
// go-elasticsearch) appear in any signature, so adapters stay swappable.
package query

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/projection"
)

// Fabric is the facade application code holds. Open() wires it from
// configured adapters; fabriqtest wires it from fakes.
type Fabric interface {
	// Exec runs one command: the only write path for aggregates.
	Exec(ctx context.Context, cmd command.Command) (command.Result, error)

	// ExecBatch runs N commands in one transaction, ordered, all-or-nothing.
	ExecBatch(ctx context.Context, cmds []command.Command) ([]command.Result, error)

	Relational() RelationalQuerier
	Graph() GraphQuerier
	Search() SearchQuerier
	Timeseries() TSQuerier
	Vector() VectorQuerier
	Document() document.Store

	// Subscribe resolves the scope to a channel server-side (authz hook
	// included) and returns a conflated delta stream.
	Subscribe(ctx context.Context, scope SubscribeScope) (<-chan Delta, error)

	// WaitForProjection blocks until the named projection has applied the
	// aggregate at or beyond version, or the context deadline expires
	// (ErrProjectionLag). It is the read-your-writes helper for callers
	// that need a projection-backed query right after a command.
	WaitForProjection(ctx context.Context, projection, aggregate, aggID string, version int64) error
}

// RelationalQuerier reads the source of truth through grove. Every method
// is tenant-scoped structurally; the grove hook backstop asserts it.
type RelationalQuerier interface {
	// Get loads one aggregate row by id into a model pointer.
	Get(ctx context.Context, entity, id string, into any) error

	// GetMany loads many rows in ONE batched query (WHERE id = ANY($1)) —
	// the dataloader-style hydration primitive. Order follows ids; missing
	// rows are skipped.
	GetMany(ctx context.Context, entity string, ids []string, into any) error

	// List pages through an entity's rows with a structured, engine-neutral
	// filter (Where/Filter), ordering and pagination. The filter covers
	// grove's builder expressiveness — operators, IN, LIKE, null checks,
	// OR groups — without leaking engine types; reads it cannot express
	// drop to the raw Query escape hatch.
	List(ctx context.Context, entity string, q ListQuery, into any) error

	// Query is the raw SQL escape hatch (still tenant-guarded). Use it for
	// reads the structured filter cannot express; writes belong to Exec.
	Query(ctx context.Context, into any, sql string, args ...any) error
}

// ListQuery selects, filters, orders and paginates an entity's rows. Its
// filter is structured and engine-neutral: Filter is the equality
// shorthand, Where is the operator-capable form (build conditions with
// Eq, Ne, Gt/Lt, In, Like/ILike, IsNull, Or, …). Filter and Where are
// ANDed together; columns are validated against the entity (an unknown
// column is rejected, which is also the injection guard).
type ListQuery struct {
	// Filter: column = value, ANDed. Equivalent to a Where of Eq's; kept
	// for the common case.
	Filter map[string]any
	// Where: operator conditions, ANDed (and ANDed with Filter).
	Where []Cond
	// OrderBy: a column, optionally suffixed " DESC". Empty orders by id.
	OrderBy string
	Limit   int
	Offset  int
}

// GraphQuerier queries the knowledge-graph projection. Cypher shipped in
// this repo must stick to the openCypher common subset — the
// adapters/graphtest conformance suite is the gate for engine swaps.
type GraphQuerier interface {
	// Query runs a read-only openCypher query. into may be *[]string for
	// single-column id traversals, or a pointer to a slice of structs for
	// multi-column rows (adapter-mapped).
	Query(ctx context.Context, cypher string, params map[string]any, into any) error

	// TraverseAndHydrate runs a traversal that RETURNs ids, then hydrates
	// the rows from Postgres in one batched relational query. The target
	// entity is inferred from into's element type via the registry. Never
	// N+1.
	TraverseAndHydrate(ctx context.Context, cypher string, params map[string]any, into any) error

	// ApplyMutations applies engine-neutral projection mutations to the
	// named target graph (projection consumers and rebuilds only).
	ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error
}

// SearchQuerier queries the full-text projection.
type SearchQuerier interface {
	// Search runs a query against an entity's declared search fields.
	Search(ctx context.Context, q SearchQuery, into any) error

	// ApplyMutations applies DocIndex/DocDeindex mutations to the named
	// target index (projection consumers and rebuilds only).
	ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error
}

// SearchQuery is a match query over an entity's declared fields.
type SearchQuery struct {
	Entity string
	Query  string
	Limit  int
}

// TSQuerier is the telemetry port (TimescaleDB hypertables). BulkWrite is
// the event-bypass ingest path: per-row events would melt the outbox, so
// bulk telemetry skips it and the relay publishes conflated deltas instead.
type TSQuerier interface {
	BulkWrite(ctx context.Context, series string, points []Point) error
	Range(ctx context.Context, q RangeQuery, into any) error
}

// Point is one telemetry sample.
type Point struct {
	Key     string // series key within the tenant, e.g. tag id
	At      time.Time
	Value   float64
	Quality int
}

// RangeQuery reads a time window of a series, optionally bucketed.
type RangeQuery struct {
	Series string
	Key    string
	From   time.Time
	To     time.Time
	Bucket time.Duration // 0 = raw points
	Agg    string        // "avg", "min", "max", "last" (when Bucket > 0)
}

// VectorQuerier is the embedding port (pgvector).
type VectorQuerier interface {
	Upsert(ctx context.Context, entity, id string, embedding []float32, meta map[string]any) error
	Similar(ctx context.Context, q VectorQuery, into any) error
}

// VectorQuery is a nearest-neighbour search.
type VectorQuery struct {
	Entity    string
	Embedding []float32
	K         int
}

// VectorMatch is one nearest-neighbour hit, best first.
type VectorMatch struct {
	ID    string
	Score float64 // cosine similarity, higher is closer
	Meta  map[string]any
}
