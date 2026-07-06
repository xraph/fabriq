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

	// OnApplied and OnFailure are optional observability hooks: OnApplied
	// fires once per envelope that is successfully applied to the Sink
	// (all writes succeeded); OnFailure fires once per envelope whose Sink
	// write returned an error. Neither fires on the skip/poison paths
	// (un-migratable payload, malformed payload, un-derivable tenant, or an
	// unmarked entity) — those are neither applied nor failed. Both are
	// nil-safe: callers that don't need metrics leave them unset. Kept as
	// bare func() (not a metrics dependency) so core/analytics stays free
	// of internal/metrics — callers wire prometheus.Counter.Inc directly.
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
		if c.OnFailure != nil {
			c.OnFailure()
		}
		return err // transient: stay pending, redelivery is safe
	}
	if err := c.Sink.AppendEvents(tctx, []Event{ev}); err != nil {
		if c.OnFailure != nil {
			c.OnFailure()
		}
		return err
	}
	if err := c.Sink.SetWatermark(tctx, []Watermark{{
		TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID, Version: env.Version,
	}}); err != nil {
		if c.OnFailure != nil {
			c.OnFailure()
		}
		return err
	}
	if c.OnApplied != nil {
		c.OnApplied()
	}
	return nil
}
