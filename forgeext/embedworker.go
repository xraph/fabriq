package forgeext

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/metrics"
	"github.com/xraph/fabriq/internal/tracing"
)

// hasEmbeddableEntity reports whether any registered entity opts into embedding.
func hasEmbeddableEntity(reg *registry.Registry) bool {
	for _, e := range reg.All() {
		if e.Spec.Embed != nil {
			return true
		}
	}
	return false
}

// hasAnalyticsEntity reports whether any registered entity opts into the
// cross-tenant analytics sink.
func hasAnalyticsEntity(reg *registry.Registry) bool {
	for _, ent := range reg.All() {
		if ent.Spec.Analytics != nil {
			return true
		}
	}
	return false
}

// hasInsightsEntity reports whether any registered entity opts into the
// per-tenant customer-facing insights projection.
func hasInsightsEntity(reg *registry.Registry) bool {
	for _, ent := range reg.All() {
		if ent.Spec.Insights != nil {
			return true
		}
	}
	return false
}

// embedHandler is the per-event consumer callback: it derives a tenant-scoped
// context from the envelope and indexes the event.
//
// Ack-skipped (returns nil, not re-queued):
//   - Tenant-less envelopes — no tenant context can be derived.
//   - Tenant context derivation failures (tenant.WithTenant error).
//   - Unindexable-payload events (agent.ErrUnindexablePayload) — structurally
//     poison; retrying will never succeed and would accumulate PEL entries.
//
// Propagated (transient → stays pending for at-least-once retry):
//   - Embedder failures and vector upsert errors.
//
// Vector upserts are idempotent by id, so at-least-once redelivery is safe.
func embedHandler(ctx context.Context, ix *agent.Indexer, m *metrics.Metrics) func(streamID string, env event.Envelope) error {
	return func(_ string, env event.Envelope) error {
		if env.TenantID == "" {
			if m != nil {
				m.EmbedEventsTotal.Inc()
			}
			return nil
		}
		tctx, err := tenant.WithTenant(ctx, env.TenantID)
		if err != nil {
			if m != nil {
				m.EmbedEventsTotal.Inc()
			}
			return nil
		}
		if !ix.Embeds(env.Aggregate) {
			// Not an embeddable entity: IndexEvent would skip it. No span,
			// so the stream's other traffic mints no traces here.
			if m != nil {
				m.EmbedEventsTotal.Inc()
			}
			return nil
		}
		// One root Consumer span per envelope embedded, linked to the
		// command that wrote it.
		tctx, span := tracing.StartConsumer(tctx, "embed event", env.Traceparent,
			telemetry.AttrConsumerGroup.String("proj:embed"),
			telemetry.AttrEntity.String(env.Aggregate),
		)
		defer span.End()
		if err := ix.IndexEvent(tctx, env); err != nil {
			if errors.Is(err, agent.ErrUnindexablePayload) {
				// skip: poison payload — retrying won't help
				span.SetAttributes(attribute.Bool("fabriq.embed.skipped", true))
				if m != nil {
					m.EmbedEventsTotal.Inc() // ack-skipped poison still counts as handled
				}
				return nil
			}
			telemetry.Fail(span, "embed failed", err)
			if m != nil {
				m.EmbedFailuresTotal.Inc()
			}
			return err
		}
		if m != nil {
			m.EmbedEventsTotal.Inc()
		}
		return nil
	}
}
