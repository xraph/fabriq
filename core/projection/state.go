package projection

import "context"

// StateReader reports how far a projection has applied a given aggregate.
// The Postgres-backed repo (phase 4) implements it from projection
// bookkeeping; fabriqtest provides an in-memory fake. WaitForProjection
// polls this port.
type StateReader interface {
	// AppliedVersion returns the latest aggregate version the projection
	// has durably applied (0 if never seen).
	AppliedVersion(ctx context.Context, tenantID, projection, aggregate, aggID string) (int64, error)
}

// State is one row of projection bookkeeping: the live pointer and stream
// position per (tenant, projection). target_name carries the blue-green
// pointer (tenant_{id}_v{N} graph or versioned index behind the alias).
type State struct {
	TenantID     string
	Projection   string // "graph" | "search"
	ModelVersion int    // bumped by rebuilds (the _v{N} suffix)
	EventVersion string // last applied event ULID / stream position
	Status       string // "live" | "building" | "soaking" | "abandoned"
	TargetName   string // engine target currently receiving applies
}

// StateRepo is the full bookkeeping port used by the projection engine,
// rebuild and reconcile (phase 4 implements it in adapters/postgres).
type StateRepo interface {
	StateReader
	Get(ctx context.Context, tenantID, projection string) (State, error)
	Upsert(ctx context.Context, s State) error
}
