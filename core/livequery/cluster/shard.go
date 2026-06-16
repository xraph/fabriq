package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/tenant"
)

// ShardDeps wires a shard. Engine is a single-node live query engine whose feed
// tails this shard's partition streams (lq:events:{p}); the shard reuses it
// unchanged and adds only ownership + the gateway protocol around it.
type ShardDeps struct {
	Engine   *livequery.Engine
	Registry livequery.SubscriptionRegistry
	Members  Membership
	Control  ControlBus
	Delta    DeltaBus
}

// ShardOptions tunes the loop cadences.
type ShardOptions struct {
	HeartbeatInterval time.Duration
	OwnershipInterval time.Duration
}

// Shard is one matcher node: it heartbeats, owns a slice of partitions by HRW,
// serves subscribe/reanchor/unsubscribe control for partitions it owns, and on
// (re)assignment rebuilds the subscriptions it now owns from the durable
// registry and re-snapshots them (a transparent failover for clients).
type Shard struct {
	id   string
	deps ShardDeps
	opts ShardOptions

	mu    sync.Mutex
	owned map[int]bool
	subs  map[string]*shardSub
}

type shardSub struct {
	reg    livequery.Registration
	handle *livequery.Handle
	stop   context.CancelFunc
}

// NewShard builds a shard.
func NewShard(id string, deps ShardDeps, opts ShardOptions) *Shard {
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 2 * time.Second
	}
	if opts.OwnershipInterval <= 0 {
		opts.OwnershipInterval = time.Second
	}
	return &Shard{id: id, deps: deps, opts: opts, owned: map[int]bool{}, subs: map[string]*shardSub{}}
}

// Run drives the shard until ctx is cancelled. It heartbeats, reconciles
// ownership, and serves control. All shard-state mutation happens on this
// goroutine; only the per-subscription delta pumps run elsewhere.
func (s *Shard) Run(ctx context.Context) error {
	ctrl, cancelCtrl, err := s.deps.Control.Control(ctx, s.id)
	if err != nil {
		return err
	}
	defer cancelCtrl()
	_ = s.deps.Members.Heartbeat(ctx, s.id) // be live before serving
	s.reconcileOwnership(ctx)

	hb := time.NewTicker(s.opts.HeartbeatInterval)
	defer hb.Stop()
	own := time.NewTicker(s.opts.OwnershipInterval)
	defer own.Stop()
	defer s.shutdown()

	for {
		select {
		case <-ctx.Done():
			_ = s.deps.Members.Leave(context.Background(), s.id)
			return nil
		case <-hb.C:
			_ = s.deps.Members.Heartbeat(ctx, s.id)
		case <-own.C:
			s.reconcileOwnership(ctx)
		case c, ok := <-ctrl:
			if !ok {
				return nil
			}
			s.handleControl(ctx, c)
		}
	}
}

func (s *Shard) handleControl(ctx context.Context, c Control) {
	switch c.Op {
	case OpSubscribe:
		p := PartitionOf(c.TenantID, c.Query.Entity)
		live, _ := s.deps.Members.LiveShards(ctx)
		if !Owns(s.id, p, live) {
			return // not ours; the gateway will re-route to the real owner
		}
		reg := livequery.Registration{
			SubID: c.SubID, TenantID: c.TenantID, Entity: c.Query.Entity,
			Partition: p, Mode: c.Query.Mode, Query: c.Query, GatewayID: c.GatewayID,
		}
		_ = s.deps.Registry.Put(ctx, reg)
		s.startSub(ctx, reg)
	case OpReanchor:
		s.mu.Lock()
		sub := s.subs[c.SubID]
		s.mu.Unlock()
		if sub == nil {
			return
		}
		snap, err := sub.handle.Reanchor(tenantCtx(ctx, sub.reg.TenantID), c.Cursor, c.Limit)
		if err != nil {
			return
		}
		sub.reg.Query.Cursor = c.Cursor
		if c.Limit > 0 {
			sub.reg.Query.Limit = c.Limit
		}
		_ = s.deps.Registry.Put(ctx, sub.reg) // persist the scroll position for failover
		s.sendSnapshot(ctx, sub.reg.GatewayID, sub.reg.SubID, snap)
	case OpUnsubscribe:
		s.mu.Lock()
		sub := s.subs[c.SubID]
		delete(s.subs, c.SubID)
		s.mu.Unlock()
		if sub != nil {
			sub.handle.Close()
			sub.stop()
			_ = s.deps.Registry.Delete(ctx, c.SubID)
		}
	}
}

// startSub subscribes in the engine, ships the snapshot to the gateway as
// deltas, and pumps live deltas to the gateway.
func (s *Shard) startSub(ctx context.Context, reg livequery.Registration) {
	tctx := tenantCtx(ctx, reg.TenantID)
	snap, deltas, handle, err := s.deps.Engine.Subscribe(tctx, reg.Query)
	if err != nil {
		return
	}
	pctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.subs[reg.SubID] = &shardSub{reg: reg, handle: handle, stop: cancel}
	s.mu.Unlock()

	s.sendSnapshot(ctx, reg.GatewayID, reg.SubID, snap)

	subID, gw := reg.SubID, reg.GatewayID // captured so the pump never touches shared state
	go func() {
		for {
			select {
			case <-pctx.Done():
				return
			case d, ok := <-deltas:
				if !ok {
					return
				}
				_ = s.deps.Delta.SendDelta(pctx, gw, GatewayDelta{SubID: subID, Delta: d})
			}
		}
	}()
}

// sendSnapshot ships a snapshot to the gateway as an OpReset followed by one
// OpEnter per row, so the client renders it on the same stream as live deltas.
func (s *Shard) sendSnapshot(ctx context.Context, gatewayID, subID string, snap livequery.Snapshot) {
	_ = s.deps.Delta.SendDelta(ctx, gatewayID, GatewayDelta{SubID: subID, Delta: livequery.LiveDelta{Op: livequery.OpReset}})
	for i, r := range snap.Rows {
		_ = s.deps.Delta.SendDelta(ctx, gatewayID, GatewayDelta{SubID: subID, Delta: livequery.LiveDelta{
			Op: livequery.OpEnter, AggID: r.AggID, Version: r.Version, Row: r.Raw, OldIndex: -1, NewIndex: i, Cursor: r.Cursor,
		}})
	}
}

func (s *Shard) reconcileOwnership(ctx context.Context) {
	live, err := s.deps.Members.LiveShards(ctx)
	if err != nil {
		return
	}
	newOwned := make(map[int]bool)
	for p := 0; p < Partitions; p++ {
		if Owns(s.id, p, live) {
			newOwned[p] = true
		}
	}
	s.mu.Lock()
	var gained, lost []int
	for p := range newOwned {
		if !s.owned[p] {
			gained = append(gained, p)
		}
	}
	for p := range s.owned {
		if !newOwned[p] {
			lost = append(lost, p)
		}
	}
	s.owned = newOwned
	s.mu.Unlock()

	for _, p := range lost {
		s.releasePartition(p)
	}
	for _, p := range gained {
		s.claimPartition(ctx, p)
	}
}

// claimPartition rebuilds (and re-snapshots) the subscriptions this shard now
// owns in partition p — the failover/rebalance recovery path.
func (s *Shard) claimPartition(ctx context.Context, p int) {
	regs, err := s.deps.Registry.ByPartitionNum(ctx, p)
	if err != nil {
		return
	}
	for _, reg := range regs {
		s.mu.Lock()
		_, exists := s.subs[reg.SubID]
		s.mu.Unlock()
		if exists {
			continue
		}
		s.startSub(ctx, reg)
	}
}

// releasePartition tears down local subscriptions for a partition this shard no
// longer owns; the new owner rebuilds them from the registry.
func (s *Shard) releasePartition(p int) {
	s.mu.Lock()
	var gone []*shardSub
	for id, sub := range s.subs {
		if sub.reg.Partition == p {
			gone = append(gone, sub)
			delete(s.subs, id)
		}
	}
	s.mu.Unlock()
	for _, sub := range gone {
		sub.handle.Close()
		sub.stop()
	}
}

func (s *Shard) shutdown() {
	s.mu.Lock()
	subs := s.subs
	s.subs = map[string]*shardSub{}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.handle.Close()
		sub.stop()
	}
}

func tenantCtx(ctx context.Context, tenantID string) context.Context {
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return ctx
	}
	return tctx
}
