package redis

import (
	"context"
	"fmt"
)

// Presence is ephemeral awareness traffic (cursors, who's-online) for
// future CRDT rooms: plain Redis pub/sub, never persisted, no delivery
// guarantees by design.

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
