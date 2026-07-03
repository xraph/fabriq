package redis

import (
	"context"
	"fmt"
	"strings"
)

// Presence is ephemeral awareness traffic (cursors, who's-online) for
// future CRDT rooms: plain Redis pub/sub, never persisted, no delivery
// guarantees by design.

// wakePrefix namespaces the sweeper's nudge channels (spec 2026-07-03 D5:
// "fabriq:wake:{tenant}").
const wakePrefix = "fabriq:wake:"

// PublishWake nudges the catalog-mode sweeper: the facade write path
// publishes it after a commit so the tenant's outbox is relayed within one
// sweep pass instead of one idle-backoff window. Fire-and-forget by design
// — the sweeper's scan cadence is the delivery guarantee, the nudge is
// only the latency optimization.
func (a *Adapter) PublishWake(ctx context.Context, tenantID string) error {
	if err := a.client.Publish(ctx, wakePrefix+tenantID, "1").Err(); err != nil {
		return fmt.Errorf("fabriq: wake publish: %w", err)
	}
	return nil
}

// SubscribeWakes delivers woken tenant ids to fn until ctx ends. ready is
// closed once the pattern subscription is active (pass nil if not needed).
func (a *Adapter) SubscribeWakes(ctx context.Context, fn func(tenantID string), ready chan<- struct{}) error {
	sub := a.client.PSubscribe(ctx, wakePrefix+"*")
	defer func() { _ = sub.Close() }()

	// Force the subscription handshake before signalling readiness.
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("fabriq: wake subscribe: %w", err)
	}
	if ready != nil {
		close(ready)
	}
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			fn(strings.TrimPrefix(msg.Channel, wakePrefix))
		}
	}
}

// PublishPresence sends one awareness payload to a room.
func (a *Adapter) PublishPresence(ctx context.Context, room string, payload []byte) error {
	if err := a.client.Publish(ctx, "presence:"+room, payload).Err(); err != nil {
		return fmt.Errorf("fabriq: presence publish: %w", err)
	}
	return nil
}

// SubscribePresence delivers room payloads to fn until ctx ends. ready is
// closed once the subscription is active (pass nil if not needed).
func (a *Adapter) SubscribePresence(ctx context.Context, room string, fn func([]byte), ready chan<- struct{}) error {
	sub := a.client.Subscribe(ctx, "presence:"+room)
	defer func() { _ = sub.Close() }()

	// Force the subscription handshake before signalling readiness.
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("fabriq: presence subscribe: %w", err)
	}
	if ready != nil {
		close(ready)
	}
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			fn([]byte(msg.Payload))
		}
	}
}
