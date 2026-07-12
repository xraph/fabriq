package insights

import (
	"context"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/otel"
)

// FactSink upserts projected facts into the tenant's own store, version-
// gated. Implemented by the postgres adapter (writes fabriq_insights_facts
// through inTenantTx, so RLS contains it). ctx carries the tenant.
type FactSink interface {
	UpsertInsightFacts(ctx context.Context, facts []Fact) error
}

// Consumer drives the proj:insights group: it reads the shared event stream
// through the exported projection.Source seam and applies each envelope to
// the FactSink. Phase 1 projects FACTS ONLY — no Events/Watermark rows (those
// are the operator-sink's concern, not the tenant's own store). Idempotency
// lives in the Sink (version gate), so redelivery is always safe.
type Consumer struct {
	Group     string // "proj:insights"
	Source    projection.Source
	Applier   *Applier
	Sink      FactSink
	Upcasters *event.UpcasterChain // optional

	// OnApplied and OnFailure are optional observability hooks, nil-safe.
	// Neither fires on the skip path (un-migratable payload, malformed
	// payload, un-derivable tenant, or an unmarked entity).
	OnApplied func()
	OnFailure func()
}

// Run consumes until ctx ends. Scale by running replicas with distinct names.
func (c *Consumer) Run(ctx context.Context, name string) error {
	if err := c.Source.EnsureGroup(ctx, c.Group); err != nil {
		return err
	}
	return c.Source.Consume(ctx, c.Group, name, func(_ string, env event.Envelope) error {
		return c.handle(ctx, env)
	})
}

func (c *Consumer) handle(ctx context.Context, env event.Envelope) error {
	if c.Upcasters != nil {
		up, err := c.Upcasters.Apply(env)
		if err != nil {
			return nil // un-migratable: skip (poison-avoidance), reconcile/backfill heals
		}
		env = up
	}
	fact, ok, err := c.Applier.Apply(env)
	if err != nil || !ok {
		return nil // malformed or not projected: skip, do not wedge the group
	}
	tctx, err := tenant.WithTenant(ctx, env.TenantID)
	if err != nil {
		return nil // un-derivable tenant can never apply
	}
	if env.ScopeID != "" {
		tctx, err = tenant.WithScope(tctx, env.ScopeID)
		if err != nil {
			return nil // un-derivable scope can never apply
		}
	}
	tctx = otel.ContextWithTraceparent(tctx, env.Traceparent)

	if err := c.Sink.UpsertInsightFacts(tctx, []Fact{fact}); err != nil {
		if c.OnFailure != nil {
			c.OnFailure()
		}
		return err // transient: stay pending, redelivery is safe (version gate is idempotent)
	}
	if c.OnApplied != nil {
		c.OnApplied()
	}
	return nil
}
