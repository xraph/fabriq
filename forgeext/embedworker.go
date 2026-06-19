package forgeext

import (
	"context"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
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

// embedHandler is the per-event consumer callback: it derives a tenant-scoped
// context from the envelope and indexes the event. A tenant-less envelope is
// skipped (returns nil) so one malformed event cannot stall the consumer.
// Vector upserts are idempotent by id, so at-least-once redelivery is safe.
func embedHandler(ctx context.Context, ix *agent.Indexer) func(streamID string, env event.Envelope) error {
	return func(_ string, env event.Envelope) error {
		if env.TenantID == "" {
			return nil
		}
		tctx, err := tenant.WithTenant(ctx, env.TenantID)
		if err != nil {
			return nil
		}
		return ix.IndexEvent(tctx, env)
	}
}
