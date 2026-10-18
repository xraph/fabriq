package analytics

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/tenant"
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

	// Observer records one duration per applied envelope under projection
	// "analytics" (optional, nil-safe).
	Observer telemetry.ApplyObserver
}

// Run consumes until ctx ends. Scale by running replicas with distinct names.
func (c *Consumer) Run(ctx context.Context, name string) error {
	// Detach the loop from any boot or request span; each applied envelope
	// gets its own root span in handle.
	ctx = telemetry.Detach(ctx)
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

	tctx, span := telemetry.StartConsumer(tctx, "apply projection event", env.Traceparent,
		telemetry.AttrProjection.String("analytics"),
		telemetry.AttrConsumerGroup.String(c.Group),
		telemetry.AttrEntity.String(env.Aggregate),
	)
	start := time.Now()
	err = c.write(tctx, env, fact, ev)
	result := telemetry.ResultOK
	if err != nil {
		result = telemetry.ResultError
		telemetry.Fail(span, "apply failed", err)
		if c.OnFailure != nil {
			c.OnFailure()
		}
	} else if c.OnApplied != nil {
		c.OnApplied()
	}
	span.End()
	if c.Observer != nil {
		c.Observer.ObserveApply("analytics", result, time.Since(start))
	}
	return err // a failure is transient: stay pending, redelivery is safe
}

// write lands one envelope's fact, history event and watermark in the Sink.
func (c *Consumer) write(ctx context.Context, env event.Envelope, fact Fact, ev Event) error {
	if err := c.Sink.UpsertFacts(ctx, []Fact{fact}); err != nil {
		return err
	}
	if err := c.Sink.AppendEvents(ctx, []Event{ev}); err != nil {
		return err
	}
	return c.Sink.SetWatermark(ctx, []Watermark{{
		TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID, Version: env.Version,
	}})
}
