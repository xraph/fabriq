package projection

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/tenant"
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

	// Observer records one duration per applied envelope (optional, nil-safe).
	Observer telemetry.ApplyObserver
}

// Run consumes until ctx ends. Scale by running replicas with distinct
// consumer names — consumer groups need no leader election.
func (e *Engine) Run(ctx context.Context, consumer string) error {
	if e.Source == nil || e.Sink == nil || e.Applier == nil || e.State == nil {
		return fmt.Errorf("fabriq: projection engine %q missing source/sink/applier/state", e.Projection)
	}
	// The loop outlives whatever span its ctx carried (a boot or request
	// span). Each envelope gets its own root span in handle instead.
	ctx = telemetry.Detach(ctx)
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

	// One root Consumer span per applied envelope, linked to the command
	// that produced it: that request has usually finished by now, so the
	// apply gets its own trace instead of hanging off an ended one.
	tctx, span := telemetry.StartConsumer(tctx, "apply projection event", env.Traceparent,
		telemetry.AttrProjection.String(e.Projection),
		telemetry.AttrConsumerGroup.String(e.Group),
		telemetry.AttrEntity.String(env.Aggregate),
		telemetry.AttrMutations.Int(len(muts)),
	)
	start := time.Now()
	err = e.apply(tctx, env, muts)
	result := telemetry.ResultOK
	if err != nil {
		result = telemetry.ResultError
		telemetry.Fail(span, "apply failed", err)
	}
	span.End()
	if e.Observer != nil {
		e.Observer.ObserveApply(e.Projection, result, time.Since(start))
	}
	return err
}

// apply writes one envelope's mutations to every target and records the
// applied version. Any error is transient: the entry stays pending.
func (e *Engine) apply(ctx context.Context, env event.Envelope, muts []Mutation) error {
	if len(muts) > 0 {
		targets := []string{""}
		if e.TargetsFor != nil {
			var err error
			targets, err = e.TargetsFor(ctx, env.TenantID)
			if err != nil {
				return err // transient (state lookup): retry via redelivery
			}
		}
		for _, target := range targets {
			if err := e.Sink.ApplyMutations(ctx, target, muts); err != nil {
				return err // transient engine failure: stay pending
			}
		}
	}

	return e.State.SetApplied(ctx, env.TenantID, e.Projection, env.Aggregate, env.AggID, env.Version)
}
