package projection

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/otel"
)

// Source is the consumer-group surface the engine reads from (implemented
// by adapters/redis).
type Source interface {
	EnsureGroup(ctx context.Context, group string) error
	Consume(ctx context.Context, group, consumer string, handle func(streamID string, env event.Envelope) error) error
}

// Sink applies translated mutations to one engine target (implemented by
// the graph/search adapters). target "" means the tenant's live target,
// resolved by the sink from the tenant on ctx.
type Sink interface {
	ApplyMutations(ctx context.Context, target string, muts []Mutation) error
}

// AppliedRecorder records per-aggregate applied versions (WaitForProjection
// reads them back). Implemented by the postgres StateRepo and fabriqtest.
type AppliedRecorder interface {
	SetApplied(ctx context.Context, tenantID, projection, aggregate, aggID string, version int64) error
}

// CustomApplier contributes extra mutations for a target beyond the built-in
// declarative applier. Apply MUST be deterministic and side-effect-free: given
// the same Envelope it must always return the same Mutations regardless of
// wall-clock time, feature flags, or any external state — this is what makes
// blue-green rebuilds produce identical projections. Specifically forbidden:
// network/database I/O, model calls, time.Now(), cross-aggregate reads, and
// randomness. A CustomApplier must be wired into BOTH Engine.Custom and the
// matching Rebuilder.Custom, or the live and rebuilt projections will diverge.
type CustomApplier struct {
	Target string // "" = any target, else must equal the engine's Projection
	Entity string // "" = any aggregate, else must equal the event's Aggregate
	Apply  Applier
}

// ApplyChain runs the built-in applier then every matching custom applier,
// unioning their mutations. Pure.
func ApplyChain(builtin Applier, custom []CustomApplier, projection string, env event.Envelope) ([]Mutation, error) {
	muts, err := builtin.Apply(env)
	if err != nil {
		return nil, err
	}
	for _, c := range custom {
		if c.Target != "" && c.Target != projection {
			continue
		}
		if c.Entity != "" && c.Entity != env.Aggregate {
			continue
		}
		extra, err := c.Apply.Apply(env)
		if err != nil {
			return nil, err
		}
		muts = append(muts, extra...)
	}
	return muts, nil
}

// Engine consumes one projection's group and applies events:
// upcast -> pure applier -> sink (per target) -> applied bookkeeping.
// Handler success acks; failure leaves the entry pending for redelivery —
// at-least-once end to end, made safe by version-gated sinks.
type Engine struct {
	Projection string // "graph" | "search"
	Group      string // "proj:graph" | "proj:search"
	Source     Source
	Sink       Sink
	Applier    Applier
	Custom     []CustomApplier      // optional; unioned after the built-in applier
	Upcasters  *event.UpcasterChain // optional; appliers see the latest shape
	State      AppliedRecorder

	// TargetsFor lists the sink targets for a tenant's events. Default:
	// [""] (live only). During a blue-green rebuild it returns the live
	// AND building targets, so live events catch the new target up while
	// the snapshot replays (version gating makes the overlap safe).
	TargetsFor func(ctx context.Context, tenantID string) ([]string, error)
}

// Run consumes until ctx ends. Scale by running replicas with distinct
// consumer names — consumer groups need no leader election.
func (e *Engine) Run(ctx context.Context, consumer string) error {
	if e.Source == nil || e.Sink == nil || e.Applier == nil || e.State == nil {
		return fmt.Errorf("fabriq: projection engine %q missing source/sink/applier/state", e.Projection)
	}
	if err := e.Source.EnsureGroup(ctx, e.Group); err != nil {
		return err
	}
	return e.Source.Consume(ctx, e.Group, consumer, func(_ string, env event.Envelope) error {
		return e.handle(ctx, env)
	})
}

func (e *Engine) handle(ctx context.Context, env event.Envelope) error {
	if e.Upcasters != nil {
		upcast, err := e.Upcasters.Apply(env)
		if err != nil {
			// A payload that cannot be migrated will never apply; leaving
			// it pending would wedge the group. Skip it — the reconciler
			// heals the aggregate from Postgres.
			return nil
		}
		env = upcast
	}

	muts, err := ApplyChain(e.Applier, e.Custom, e.Projection, env)
	if err != nil {
		return nil // malformed payload: same poison-avoidance as above
	}

	tctx, err := tenant.WithTenant(ctx, env.TenantID)
	if err != nil {
		return nil // un-derivable tenant can never apply; skip
	}
	tctx = otel.ContextWithTraceparent(tctx, env.Traceparent)

	if len(muts) > 0 {
		targets := []string{""}
		if e.TargetsFor != nil {
			targets, err = e.TargetsFor(tctx, env.TenantID)
			if err != nil {
				return err // transient (state lookup): retry via redelivery
			}
		}
		for _, target := range targets {
			if err := e.Sink.ApplyMutations(tctx, target, muts); err != nil {
				return err // transient engine failure: stay pending
			}
		}
	}

	return e.State.SetApplied(tctx, env.TenantID, e.Projection, env.Aggregate, env.AggID, env.Version)
}
