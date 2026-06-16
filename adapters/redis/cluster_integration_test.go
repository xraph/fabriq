//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/cluster"
)

func TestRedisCluster_Transports(t *testing.T) {
	a := openRedis(t)
	ct := a.Cluster(2 * time.Second)
	ctx := context.Background()

	// Membership: two heartbeats → both live; Leave removes one.
	if err := ct.Heartbeat(ctx, "shard-x"); err != nil {
		t.Fatal(err)
	}
	if err := ct.Heartbeat(ctx, "shard-y"); err != nil {
		t.Fatal(err)
	}
	live, err := ct.LiveShards(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 {
		t.Fatalf("live shards = %v, want 2", live)
	}
	if err := ct.Leave(ctx, "shard-x"); err != nil {
		t.Fatal(err)
	}
	if live, _ = ct.LiveShards(ctx); len(live) != 1 || live[0] != "shard-y" {
		t.Fatalf("after leave, live = %v, want [shard-y]", live)
	}

	// Control roundtrip (gateway → owning shard).
	cch, ccancel, err := ct.Control(ctx, "shard-y")
	if err != nil {
		t.Fatal(err)
	}
	defer ccancel()
	time.Sleep(200 * time.Millisecond) // let the XREAD("$") reader attach
	if err := ct.SendControl(ctx, "shard-y", cluster.Control{
		Op: cluster.OpSubscribe, SubID: "s1", GatewayID: "gw1", TenantID: "acme",
		Query: livequery.LiveQuery{Entity: "asset"},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-cch:
		if c.Op != cluster.OpSubscribe || c.SubID != "s1" || c.Query.Entity != "asset" {
			t.Fatalf("control roundtrip wrong: %+v", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no control message received")
	}

	// Delta roundtrip (shard → gateway).
	dch, dcancel, err := ct.Deltas(ctx, "gw1")
	if err != nil {
		t.Fatal(err)
	}
	defer dcancel()
	time.Sleep(200 * time.Millisecond)
	if err := ct.SendDelta(ctx, "gw1", cluster.GatewayDelta{
		SubID: "s1", Delta: livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a1", NewIndex: 0, OldIndex: -1},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-dch:
		if d.SubID != "s1" || d.Delta.Op != livequery.OpEnter || d.Delta.AggID != "a1" {
			t.Fatalf("delta roundtrip wrong: %+v", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no delta received")
	}
}
