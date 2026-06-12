package projection

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/event"
)

// PHASE 4 SCAFFOLD — the consumer loop that drives projections.
//
// The primitives it composes already exist and are tested: the Redis
// adapter's consumer groups (EnsureGroup/Consume with XAUTOCLAIM
// recovery), the registry-derived appliers (GraphApplier/SearchApplier),
// the engine-neutral mutations, the upcaster chain, and the StateRepo
// (applied-version bookkeeping). What remains is the wiring below plus
// the target resolution against blue-green state.

// Source is the consumer-group surface the engine reads from (implemented
// by adapters/redis).
type Source interface {
	EnsureGroup(ctx context.Context, group string) error
	Consume(ctx context.Context, group, consumer string, handle func(streamID string, env event.Envelope) error) error
}

// Sink applies translated mutations to one engine target (implemented by
// the graph/search adapters' ApplyMutations).
type Sink interface {
	ApplyMutations(ctx context.Context, target string, muts []Mutation) error
}

// Engine consumes one projection's group and applies events.
type Engine struct {
	Projection string // "graph" | "search"
	Group      string // "proj:graph" | "proj:search"
	Source     Source
	Sink       Sink
	Applier    Applier
	Upcasters  *event.UpcasterChain
	State      StateRepo
	// TargetFor resolves the engine target for a tenant from blue-green
	// state (live graph name / index alias, or the building _v{N+1}).
	TargetFor func(ctx context.Context, tenantID string) (string, error)
}

// Run is the consumer loop (TODO, phase 4):
//
//  1. Source.EnsureGroup(Group)
//  2. Source.Consume(group, consumer, handle) where handle:
//     a. env = Upcasters.Apply(env)            // appliers see latest shape
//     b. muts = Applier.Apply(env)             // pure translation
//     c. target = TargetFor(env.TenantID)      // blue-green aware
//     d. Sink.ApplyMutations(target, muts)     // version-gated, idempotent
//     e. State.SetApplied(tenant, projection, aggregate, aggID, version)
//     f. return nil -> ack; error -> stay pending (at-least-once)
//  3. During rebuilds the engine ALSO applies to the building target from
//     the snapshot watermark (rebuild.go owns that orchestration).
func (e *Engine) Run(ctx context.Context, consumer string) error {
	_ = ctx
	_ = consumer
	return fmt.Errorf("fabriq: projection engine not implemented yet (phase 4)")
}
