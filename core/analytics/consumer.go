package analytics

import (
	"context"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/otel"
)

// Consumer drives the proj:analytics group: it reads the shared event stream
// through the exported projection.Source seam and applies each envelope to the
// analytics Sink. It deliberately does NOT reuse projection.Engine, which is
// hardwired to the closed []Mutation set and graph/search-specific
// AppliedRecorder/TargetsFor. Idempotency lives in the Sink (version gate), so
// there is no per-tenant applied bookkeeping and the apply path never touches
// a tenant database.
type Consumer struct {
	Group     string // "proj:analytics"
	Source    projection.Source
	Applier   *Applier
	Sink      Sink
	Upcasters *event.UpcasterChain // optional
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
			return nil // un-migratable payload: skip (poison-avoidance), reconcile/backfill heals
		}
		env = up
	}
	fact, ev, ok, err := c.Applier.Apply(env)
	if err != nil {
		return nil // malformed payload: skip, do not wedge the group
	}
	if !ok {
		return nil // not analyticized
	}
	tctx, err := tenant.WithTenant(ctx, env.TenantID)
	if err != nil {
		return nil // un-derivable tenant can never apply
	}
	tctx = otel.ContextWithTraceparent(tctx, env.Traceparent)

	if err := c.Sink.UpsertFacts(tctx, []Fact{fact}); err != nil {
		return err // transient: stay pending, redelivery is safe
	}
	if err := c.Sink.AppendEvents(tctx, []Event{ev}); err != nil {
		return err
	}
	return c.Sink.SetWatermark(tctx, []Watermark{{
		TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID, Version: env.Version,
	}})
}
