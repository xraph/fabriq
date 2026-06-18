package cache

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
)

// eventTailer is the broadcast source of committed envelopes (the redis adapter
// implements it via TailEvents over the main stream).
type eventTailer interface {
	TailEvents(ctx context.Context, handle func(event.Envelope) error) error
}

// RunL1EvictTailer drives per-node L1 eviction from the event stream: every
// committed change evicts this node's L1 for the changed entity + id. Blocks
// until ctx ends. It is best-effort — a handler error on one envelope is logged
// (here: skipped) and tailing continues; the L1 TTL is the backstop.
func RunL1EvictTailer(ctx context.Context, t eventTailer, l *L1Cache) error {
	return t.TailEvents(ctx, func(env event.Envelope) error {
		ectx, err := evictCtx(ctx, env.TenantID, env.ScopeID)
		if err != nil {
			return nil // malformed tenant/scope: skip, do not abort the tail
		}
		l.EvictLocal(ectx, env.Aggregate, env.AggID)
		return nil
	})
}

// evictCtx rebuilds a tenant/scope-scoped context from an envelope so
// EvictLocal resolves the same partitions the write used.
func evictCtx(parent context.Context, tenantID, scopeID string) (context.Context, error) {
	c, err := tenant.WithTenant(parent, tenantID)
	if err != nil {
		return nil, fmt.Errorf("fabriq/cache: evict ctx: %w", err)
	}
	if scopeID != "" {
		c, err = tenant.WithScope(c, scopeID)
		if err != nil {
			return nil, fmt.Errorf("fabriq/cache: evict scope: %w", err)
		}
	}
	return c, nil
}
