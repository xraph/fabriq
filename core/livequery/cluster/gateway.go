package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/tenant"
)

// GatewayDeps wires a gateway.
type GatewayDeps struct {
	Members Membership
	Control ControlBus
	Delta   DeltaBus
}

// Gateway terminates client subscriptions: it routes control to the owning
// shard and demultiplexes its delta channel back to per-subscription streams.
// (This is the thin in-process gateway; a standalone SSE/WS gateway process
// builds on the same protocol.)
type Gateway struct {
	id   string
	deps GatewayDeps

	mu   sync.Mutex
	subs map[string]chan livequery.LiveDelta
	seq  uint64
}

// NewGateway builds a gateway.
func NewGateway(id string, deps GatewayDeps) *Gateway {
	return &Gateway{id: id, deps: deps, subs: map[string]chan livequery.LiveDelta{}}
}

// Run pumps this gateway's delta channel, demuxing each frame to the right
// subscription stream, until ctx is cancelled.
func (g *Gateway) Run(ctx context.Context) error {
	in, cancel, err := g.deps.Delta.Deltas(ctx, g.id)
	if err != nil {
		return err
	}
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case gd, ok := <-in:
			if !ok {
				return nil
			}
			g.mu.Lock()
			out := g.subs[gd.SubID]
			g.mu.Unlock()
			if out != nil {
				select {
				case out <- gd.Delta:
				default: // consumer behind: drop (the next OpReset re-syncs it)
				}
			}
		}
	}
}

// Subscribe registers a live query: it routes a subscribe control to the shard
// that owns the query's partition and returns the demuxed delta stream. The
// initial snapshot arrives on that stream as an OpReset followed by OpEnter
// rows (the same encoding a failover re-snapshot uses).
func (g *Gateway) Subscribe(ctx context.Context, q livequery.LiveQuery) (id string, stream <-chan livequery.LiveDelta, release func(), retErr error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	live, err := g.deps.Members.LiveShards(ctx)
	if err != nil {
		return "", nil, nil, err
	}
	p := PartitionOf(tid, q.Entity)
	owner := Owner(p, live)
	if owner == "" {
		return "", nil, nil, fmt.Errorf("fabriq: no live shard for partition %d", p)
	}

	subID := fmt.Sprintf("%s-%d", g.id, atomic.AddUint64(&g.seq, 1))
	out := make(chan livequery.LiveDelta, 64)
	g.mu.Lock()
	g.subs[subID] = out
	g.mu.Unlock()

	if err := g.deps.Control.SendControl(ctx, owner, Control{
		Op: OpSubscribe, SubID: subID, GatewayID: g.id, TenantID: tid, Query: q,
	}); err != nil {
		g.mu.Lock()
		delete(g.subs, subID)
		g.mu.Unlock()
		return "", nil, nil, err
	}

	cancel := func() {
		g.mu.Lock()
		delete(g.subs, subID)
		g.mu.Unlock()
		live, _ := g.deps.Members.LiveShards(context.Background())
		if o := Owner(p, live); o != "" {
			_ = g.deps.Control.SendControl(context.Background(), o, Control{Op: OpUnsubscribe, SubID: subID})
		}
	}
	return subID, out, cancel, nil
}

// Reanchor routes a reanchor control to the current owner of the subscription's
// partition (recomputed, so it survives a failover).
func (g *Gateway) Reanchor(ctx context.Context, subID string, q livequery.LiveQuery, cursor *livequery.Cursor, limit int) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	live, err := g.deps.Members.LiveShards(ctx)
	if err != nil {
		return err
	}
	owner := Owner(PartitionOf(tid, q.Entity), live)
	if owner == "" {
		return fmt.Errorf("fabriq: no live shard for reanchor")
	}
	return g.deps.Control.SendControl(ctx, owner, Control{
		Op: OpReanchor, SubID: subID, GatewayID: g.id, TenantID: tid, Query: q, Cursor: cursor, Limit: limit,
	})
}
