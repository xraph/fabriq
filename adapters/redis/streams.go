package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
)

// envField is the single stream-entry field carrying the encoded envelope.
const envField = "env"

var (
	_ event.Publisher  = (*Adapter)(nil)
	_ subscribe.Tailer = (*Adapter)(nil)
)

// Publish implements event.Publisher: one XADD to the main event stream
// (consumed by projection groups) plus one per derived change channel
// (short MAXLEN~; reconnecting clients catch up from Last-Event-ID or fall
// back to refetch).
func (a *Adapter) Publish(ctx context.Context, env event.Envelope, channels []string) (string, error) {
	raw, err := event.Encode(env)
	if err != nil {
		return "", err
	}

	pipe := a.client.Pipeline()
	eventsAdd := pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: registry.StreamKey(),
		MaxLen: a.eventsMaxLen,
		Approx: true,
		Values: map[string]any{envField: raw},
	})
	for _, ch := range channels {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: ch,
			MaxLen: a.chanMaxLen,
			Approx: true,
			Values: map[string]any{envField: raw},
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("fabriq: publish event %s: %w", env.ID, err)
	}
	return eventsAdd.Val(), nil
}

// Tail implements subscribe.Tailer: blocking XREAD on one change channel,
// delivering every entry after fromID until ctx ends.
func (a *Adapter) Tail(ctx context.Context, channel, fromID string, deliver func(query.Delta)) error {
	last := fromID
	if last == "" {
		last = "$"
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := a.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{channel, last},
			Count:   64,
			Block:   time.Second,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil):
			continue // block timeout, poll again
		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fabriq: tail %s: %w", channel, err)
		}
		for _, stream := range res {
			for _, entry := range stream.Messages {
				last = entry.ID
				d, ok := deltaFromEntry(channel, entry)
				if ok {
					deliver(d)
				}
			}
		}
	}
}

// ReadRange implements subscribe.Tailer's catch-up read: entries strictly
// after afterID ("0" reads from the beginning).
func (a *Adapter) ReadRange(ctx context.Context, channel, afterID string, limit int) ([]query.Delta, error) {
	if limit <= 0 {
		limit = 500
	}
	start := "-"
	if afterID != "" && afterID != "0" {
		start = "(" + afterID
	}
	entries, err := a.client.XRangeN(ctx, channel, start, "+", int64(limit)).Result()
	if err != nil {
		return nil, fmt.Errorf("fabriq: read range %s: %w", channel, err)
	}
	out := make([]query.Delta, 0, len(entries))
	for _, entry := range entries {
		if d, ok := deltaFromEntry(channel, entry); ok {
			out = append(out, d)
		}
	}
	return out, nil
}

// GroupLag reports how many event-stream entries a consumer group has not
// yet processed (Redis 7 XINFO GROUPS lag + pending).
func (a *Adapter) GroupLag(ctx context.Context, group string) (int64, error) {
	groups, err := a.client.XInfoGroups(ctx, registry.StreamKey()).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return 0, nil
		}
		return 0, fmt.Errorf("fabriq: group lag: %w", err)
	}
	for _, g := range groups {
		if g.Name == group {
			return g.Lag + g.Pending, nil
		}
	}
	return 0, nil
}

// EnsureGroup creates a projection consumer group on the event stream
// (idempotent), starting from the beginning so a new projection replays
// what the stream still holds.
func (a *Adapter) EnsureGroup(ctx context.Context, group string) error {
	err := a.client.XGroupCreateMkStream(ctx, registry.StreamKey(), group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("fabriq: ensure group %s: %w", group, err)
	}
	return nil
}

// Consume runs a consumer-group loop on the event stream: claimed-pending
// entries first (crash recovery), then new entries. Handler success acks;
// handler failure leaves the entry pending for redelivery (at-least-once).
// Returns when ctx ends.
func (a *Adapter) Consume(ctx context.Context, group, consumer string, handle func(streamID string, env event.Envelope) error) error {
	// Recover entries other consumers left pending (crashed replicas).
	if err := a.claimStale(ctx, group, consumer, handle); err != nil && ctx.Err() == nil {
		return err
	}
	readID := ">"
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{registry.StreamKey(), readID},
			Count:    64,
			Block:    time.Second,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil):
			continue
		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fabriq: consume %s: %w", group, err)
		}
		for _, stream := range res {
			for _, entry := range stream.Messages {
				a.handleEntry(ctx, group, entry, handle)
			}
		}
	}
}

// claimStale takes over pending entries older than a second (their owner
// is gone or wedged) and processes them.
func (a *Adapter) claimStale(ctx context.Context, group, consumer string, handle func(string, event.Envelope) error) error {
	start := "0-0"
	for {
		entries, next, err := a.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   registry.StreamKey(),
			Group:    group,
			Consumer: consumer,
			MinIdle:  time.Second,
			Start:    start,
			Count:    64,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "NOGROUP") {
				return nil
			}
			return fmt.Errorf("fabriq: autoclaim %s: %w", group, err)
		}
		for _, entry := range entries {
			a.handleEntry(ctx, group, entry, handle)
		}
		if next == "0-0" || len(entries) == 0 {
			return nil
		}
		start = next
	}
}

func (a *Adapter) handleEntry(ctx context.Context, group string, entry redis.XMessage, handle func(string, event.Envelope) error) {
	env, ok := envelopeFromEntry(entry)
	if !ok {
		// Malformed entry: ack it away rather than wedging the group.
		_ = a.client.XAck(ctx, registry.StreamKey(), group, entry.ID).Err()
		return
	}
	if err := handle(entry.ID, env); err != nil {
		return // no ack: stays pending for redelivery
	}
	_ = a.client.XAck(ctx, registry.StreamKey(), group, entry.ID).Err()
}

func envelopeFromEntry(entry redis.XMessage) (event.Envelope, bool) {
	raw, ok := entry.Values[envField].(string)
	if !ok {
		return event.Envelope{}, false
	}
	env, err := event.Decode([]byte(raw))
	if err != nil {
		return event.Envelope{}, false
	}
	return env, true
}

func deltaFromEntry(channel string, entry redis.XMessage) (query.Delta, bool) {
	env, ok := envelopeFromEntry(entry)
	if !ok {
		return query.Delta{}, false
	}
	return query.DeltaFromEnvelope(channel, entry.ID, env), true
}
