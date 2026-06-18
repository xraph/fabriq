package livequery

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/xraph/fabriq/core/query"
)

// Tailer is the subset of the redis adapter the feed needs.
type Tailer interface {
	Tail(ctx context.Context, channel, fromID string, deliver func(query.Delta)) error
}

// ChannelFor resolves the coarse event channel a live query tails in P1
// (the entity's by-tenant scope channel); P3 replaces this with partition
// streams. It takes ctx because the tenant comes from the authenticated
// context, never the client.
type ChannelFor func(ctx context.Context, q LiveQuery) (channel string, err error)

// RedisFeed adapts the Redis Tailer into the engine Feed: it converts each
// query.Delta on the channel into a Change (decoding the column-keyed payload),
// dropping events for other entities that share the coarse by-tenant channel.
type RedisFeed struct {
	tail    Tailer
	channel ChannelFor
}

// NewRedisFeed builds a feed over a Redis tailer and a channel resolver.
func NewRedisFeed(tail Tailer, channel ChannelFor) *RedisFeed {
	return &RedisFeed{tail: tail, channel: channel}
}

// Changes implements Feed.
func (f *RedisFeed) Changes(ctx context.Context, q LiveQuery, from string) (stream <-chan Change, stop func(), retErr error) {
	ch, err := f.channel(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan Change, 64)
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		_ = f.tail.Tail(cctx, ch, from, func(d query.Delta) {
			if d.Aggregate != q.Entity {
				return // by-tenant channel is shared across entities; keep only ours
			}
			c := Change{
				AggID:    d.AggID,
				Version:  d.Version,
				StreamID: d.StreamID,
				At:       d.At,
				Raw:      d.Payload,
				Deleted:  isDeleted(d.Type),
			}
			if !c.Deleted {
				var vals map[string]any
				if json.Unmarshal(d.Payload, &vals) == nil {
					c.Vals = vals
				}
			}
			select {
			case out <- c:
			case <-cctx.Done():
			}
		})
	}()
	return out, cancel, nil
}

func isDeleted(eventType string) bool {
	return strings.HasSuffix(eventType, ".deleted")
}
