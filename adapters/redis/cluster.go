package redis

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/livequery/cluster"
)

// ClusterTransport backs the sharded live query tier on Redis: shard liveness
// via TTL heartbeat keys (cluster.Membership), gateway→shard control via
// per-shard request streams (cluster.ControlBus), and shard→gateway deltas via
// per-gateway streams (cluster.DeltaBus).
type ClusterTransport struct {
	client *redis.Client
	ttl    time.Duration
	maxLen int64
}

// Cluster returns the Redis-backed cluster transport. ttl is the heartbeat
// liveness window (default 6s); a shard must refresh well within it.
func (a *Adapter) Cluster(ttl time.Duration) *ClusterTransport {
	if ttl <= 0 {
		ttl = 6 * time.Second
	}
	return &ClusterTransport{client: a.client, ttl: ttl, maxLen: 10000}
}

var (
	_ cluster.Membership = (*ClusterTransport)(nil)
	_ cluster.ControlBus = (*ClusterTransport)(nil)
	_ cluster.DeltaBus   = (*ClusterTransport)(nil)
)

// --- Membership ---

func (c *ClusterTransport) Heartbeat(ctx context.Context, shardID string) error {
	return c.client.Set(ctx, cluster.HeartbeatKey(shardID), "1", c.ttl).Err()
}

func (c *ClusterTransport) Leave(ctx context.Context, shardID string) error {
	return c.client.Del(ctx, cluster.HeartbeatKey(shardID)).Err()
}

func (c *ClusterTransport) LiveShards(ctx context.Context) ([]string, error) {
	prefix := cluster.HeartbeatKey("")
	var ids []string
	iter := c.client.Scan(ctx, 0, prefix+"*", 200).Iterator()
	for iter.Next(ctx) {
		ids = append(ids, strings.TrimPrefix(iter.Val(), prefix))
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	return ids, nil
}

// --- ControlBus ---

func (c *ClusterTransport) SendControl(ctx context.Context, shardID string, ctrl cluster.Control) error {
	raw, err := json.Marshal(ctrl)
	if err != nil {
		return err
	}
	return c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: cluster.CtrlStream(shardID), MaxLen: c.maxLen, Approx: true,
		Values: map[string]any{"c": raw},
	}).Err()
}

func (c *ClusterTransport) Control(ctx context.Context, shardID string) (<-chan cluster.Control, func(), error) {
	out := make(chan cluster.Control, 64)
	cctx, cancel := context.WithCancel(ctx)
	go readStream(cctx, c.client, cluster.CtrlStream(shardID), "c", func(raw []byte) {
		var ctrl cluster.Control
		if json.Unmarshal(raw, &ctrl) == nil {
			select {
			case out <- ctrl:
			case <-cctx.Done():
			}
		}
	}, func() { close(out) })
	return out, cancel, nil
}

// --- DeltaBus ---

func (c *ClusterTransport) SendDelta(ctx context.Context, gatewayID string, d cluster.GatewayDelta) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: cluster.DeltaChannel(gatewayID), MaxLen: c.maxLen, Approx: true,
		Values: map[string]any{"d": raw},
	}).Err()
}

func (c *ClusterTransport) Deltas(ctx context.Context, gatewayID string) (<-chan cluster.GatewayDelta, func(), error) {
	out := make(chan cluster.GatewayDelta, 256)
	cctx, cancel := context.WithCancel(ctx)
	go readStream(cctx, c.client, cluster.DeltaChannel(gatewayID), "d", func(raw []byte) {
		var gd cluster.GatewayDelta
		if json.Unmarshal(raw, &gd) == nil {
			select {
			case out <- gd:
			case <-cctx.Done():
			}
		}
	}, func() { close(out) })
	return out, cancel, nil
}

// readStream blocks on XREAD over one stream from "now", decoding the named
// field of each entry, until ctx ends. Transient errors retry; the closer runs
// on exit.
func readStream(ctx context.Context, client *redis.Client, stream, field string, deliver func([]byte), closer func()) {
	defer closer()
	last := "$"
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, last}, Count: 64, Block: time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, redis.Nil) {
				if ctx.Err() != nil {
					return
				}
				continue // no new entries this block window
			}
			continue // transient
		}
		for _, s := range res {
			for _, m := range s.Messages {
				last = m.ID
				if v, ok := m.Values[field].(string); ok {
					deliver([]byte(v))
				}
			}
		}
	}
}
