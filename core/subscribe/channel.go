// Package subscribe is fabriq's subscription plane: server-side channel
// resolution, the subscribe-time authorization gate, the conflating
// fan-out hub, and the SSE bridge.
//
// Channel names are derived exclusively here (via core/registry): client
// input never names a channel or a tenant.
package subscribe

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// ResolveChannel maps a validated subscription request onto its channel
// name, taking the tenant from the authenticated context only.
func ResolveChannel(ctx context.Context, reg *registry.Registry, req query.SubscribeScope) (string, error) {
	tenantID, err := tenant.Require(ctx)
	if err != nil {
		return "", err
	}
	ent, ok := reg.Get(req.Entity)
	if !ok {
		return "", fmt.Errorf("fabriq: unknown entity %q", req.Entity)
	}
	for _, s := range ent.Spec.Subscribe {
		if s.Name != req.Scope {
			continue
		}
		if s == registry.ByTenant {
			// The tenant scope id is always the context tenant; any
			// client-supplied id is ignored by construction.
			return registry.ChannelName(tenantID, s, tenantID), nil
		}
		if req.ID == "" {
			return "", fmt.Errorf("fabriq: scope %q on %q requires an id", req.Scope, req.Entity)
		}
		return registry.ChannelName(tenantID, s, req.ID), nil
	}
	return "", fmt.Errorf("fabriq: entity %q does not declare subscription scope %q", req.Entity, req.Scope)
}

// ChannelsForEnvelope derives every channel an event must be published to:
// the entity channel plus each containing-scope channel whose field is
// present and non-empty in the payload, in spec declaration order. Events
// for unregistered entities derive no channels.
func ChannelsForEnvelope(reg *registry.Registry, env event.Envelope) ([]string, error) {
	ent, ok := reg.Get(env.Aggregate)
	if !ok {
		return nil, nil
	}
	var payload map[string]any
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return nil, fmt.Errorf("fabriq: event %s payload not an object: %w", env.ID, err)
		}
	}
	channels := make([]string, 0, len(ent.Spec.Subscribe))
	for _, s := range ent.Spec.Subscribe {
		switch s {
		case registry.ByID:
			channels = append(channels, registry.ChannelName(env.TenantID, s, env.AggID))
		case registry.ByTenant:
			channels = append(channels, registry.ChannelName(env.TenantID, s, env.TenantID))
		default:
			if id, ok := payload[s.Field].(string); ok && id != "" {
				channels = append(channels, registry.ChannelName(env.TenantID, s, id))
			}
		}
	}
	return channels, nil
}
