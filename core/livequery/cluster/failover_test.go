package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// TestCluster_FailoverRebuildsSubscriptions is the multi-process (goroutines
// over an in-memory bus + shared store) failover proof: a subscription served
// by one shard survives that shard's death — the new owner rebuilds it from the
// durable registry, re-snapshots (the client sees OpReset), and keeps serving.
func TestCluster_FailoverRebuildsSubscriptions(t *testing.T) {
	bus := newMemBus()
	store := newMemStore()
	opts := ShardOptions{HeartbeatInterval: 100 * time.Millisecond, OwnershipInterval: 40 * time.Millisecond}

	rootCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	shardCancel := map[string]context.CancelFunc{}
	for _, id := range []string{"shard-a", "shard-b", "shard-c"} {
		eng := livequery.NewEngine(store, store, bus.feed(id), livequery.EngineOptions{Cushion: 4, Buffer: 64})
		sh := NewShard(id, ShardDeps{Engine: eng, Registry: store, Members: bus, Control: bus, Delta: bus}, opts)
		sctx, scancel := context.WithCancel(rootCtx)
		shardCancel[id] = scancel
		go func() { _ = sh.Run(sctx) }()
	}

	gw := NewGateway("gw1", GatewayDeps{Members: bus, Control: bus, Delta: bus})
	go func() { _ = gw.Run(rootCtx) }()

	time.Sleep(180 * time.Millisecond) // let heartbeats + ownership settle

	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	q := livequery.LiveQuery{
		Entity: "asset", Sort: []livequery.SortKey{{Column: "name"}}, Limit: 10,
		Where: query.Where{query.Eq("kind", "pump")},
	}
	_, deltas, cancelSub, err := gw.Subscribe(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelSub()

	// Initial snapshot: OpReset over an empty matching set.
	expectOp(t, deltas, livequery.OpReset, 2*time.Second)

	// A matching write reaches the owning shard and the client.
	writeRow(bus, store, "p1", "acme", "asset", "Alpha", "pump", 1)
	expectEnter(t, deltas, "p1", 2*time.Second)

	// Kill the shard that owns this partition.
	owner := Owner(PartitionOf("acme", "asset"), liveOf(bus))
	bus.kill(owner)
	shardCancel[owner]()

	// Failover: the surviving owner rebuilds from the registry and re-snapshots,
	// so the SAME client stream gets an OpReset.
	expectOp(t, deltas, livequery.OpReset, 4*time.Second)

	// The new owner serves live: a subsequent write still reaches the client.
	writeRow(bus, store, "p2", "acme", "asset", "Bravo", "pump", 1)
	expectEnter(t, deltas, "p2", 4*time.Second)
}

func liveOf(bus *memBus) []string {
	live, _ := bus.LiveShards(context.Background())
	return live
}

func writeRow(bus *memBus, store *memStore, id, tenantID, entity, name, kind string, v int64) {
	vals := map[string]any{"id": id, "tenant_id": tenantID, "name": name, "kind": kind}
	raw, _ := json.Marshal(vals)
	store.setRow(id, v, vals)
	bus.publishChange(tenantID, entity, livequery.Change{AggID: id, Version: v, Vals: vals, Raw: raw})
}

func expectOp(t *testing.T, ch <-chan livequery.LiveDelta, op livequery.DeltaOp, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-ch:
			if d.Op == op {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for op %v", op)
		}
	}
}

func expectEnter(t *testing.T, ch <-chan livequery.LiveDelta, aggID string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case d := <-ch:
			if d.Op == livequery.OpEnter && d.AggID == aggID {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for enter(%s)", aggID)
		}
	}
}
