// Package cluster holds the engine-neutral coordination primitives for the
// sharded live query matcher tier: how data maps to partitions, and how live
// shards divide those partitions among themselves by rendezvous (HRW) hashing.
// It depends on nothing engine-specific; Redis-backed membership and the shard
// runtime live alongside it / in adapters.
package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// Partitions is the fixed number of data partitions. A change and the
// subscriptions over its (tenant, entity) always land on the same partition, so
// one shard owns both. Re-partitioning is an operational migration.
const Partitions = 256

// PartitionOf maps a (tenant, entity) to its partition. Deterministic and
// process-independent (FNV-1a), so every node computes the same routing.
func PartitionOf(tenantID, entity string) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(entity))
	return int(h.Sum64() % Partitions)
}

// EventStream is the Redis stream a partition's events are published to and the
// owning shard tails.
func EventStream(p int) string { return fmt.Sprintf("lq:events:%d", p) }

// CtrlStream is the per-shard control request stream (subscribe/reanchor/...).
func CtrlStream(shardID string) string { return "lq:ctrl:" + shardID }

// DeltaChannel is the per-gateway delta stream the shards publish to.
func DeltaChannel(gatewayID string) string { return "lq:delta:" + gatewayID }

// HeartbeatKey is a shard's liveness key.
func HeartbeatKey(shardID string) string { return "lq:shard:" + shardID }

// Owner returns the shard that owns partition p among the live set: the shard
// maximizing the rendezvous weight hash(shard, p). Returns "" if live is empty.
// Pure given the live set, so every node agrees without a coordinator, and
// removing a shard reassigns only that shard's partitions.
func Owner(p int, live []string) string {
	best := ""
	var bestW uint64
	for _, s := range live {
		w := weight(s, p)
		if best == "" || w > bestW || (w == bestW && s < best) {
			best, bestW = s, w
		}
	}
	return best
}

// Owns reports whether self owns partition p among the live set.
func Owns(self string, p int, live []string) bool { return Owner(p, live) == self }

func weight(shardID string, p int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(shardID))
	_, _ = h.Write([]byte{0})
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(p)) // #nosec G115 -- p is a non-negative partition index
	_, _ = h.Write(buf[:])
	return h.Sum64()
}

// Membership is the liveness substrate: shards refresh a TTL heartbeat, and any
// node can read the currently-live set. Redis backs it in production.
type Membership interface {
	// Heartbeat refreshes this shard's TTL liveness key; call it on a ticker
	// at an interval well under the TTL.
	Heartbeat(ctx context.Context, shardID string) error
	// LiveShards returns the currently-live shard ids (unexpired heartbeats),
	// sorted, so Owner is stable across callers.
	LiveShards(ctx context.Context) ([]string, error)
	// Leave removes this shard's heartbeat immediately (graceful shutdown).
	Leave(ctx context.Context, shardID string) error
}
