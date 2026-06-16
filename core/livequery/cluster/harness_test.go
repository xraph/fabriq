package cluster

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/tenant"
)

// --- in-memory bus: Membership + ControlBus + DeltaBus + per-shard event feeds.

type memBus struct {
	mu    sync.Mutex
	alive map[string]bool
	ctrl  map[string]chan Control
	delta map[string]chan GatewayDelta
	feeds map[string]*memFeed
}

func newMemBus() *memBus {
	return &memBus{
		alive: map[string]bool{}, ctrl: map[string]chan Control{},
		delta: map[string]chan GatewayDelta{}, feeds: map[string]*memFeed{},
	}
}

func (b *memBus) Heartbeat(_ context.Context, id string) error {
	b.mu.Lock()
	b.alive[id] = true
	b.mu.Unlock()
	return nil
}

func (b *memBus) Leave(_ context.Context, id string) error {
	b.mu.Lock()
	delete(b.alive, id)
	b.mu.Unlock()
	return nil
}

// kill simulates a node death: the shard stops being live immediately.
func (b *memBus) kill(id string) {
	b.mu.Lock()
	delete(b.alive, id)
	b.mu.Unlock()
}

func (b *memBus) LiveShards(_ context.Context) ([]string, error) {
	b.mu.Lock()
	out := make([]string, 0, len(b.alive))
	for id, ok := range b.alive {
		if ok {
			out = append(out, id)
		}
	}
	b.mu.Unlock()
	sort.Strings(out)
	return out, nil
}

func (b *memBus) ctrlChan(id string) chan Control {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := b.ctrl[id]
	if ch == nil {
		ch = make(chan Control, 64)
		b.ctrl[id] = ch
	}
	return ch
}

func (b *memBus) SendControl(_ context.Context, id string, c Control) error {
	select {
	case b.ctrlChan(id) <- c:
	default:
	}
	return nil
}

func (b *memBus) Control(_ context.Context, id string) (<-chan Control, func(), error) {
	return b.ctrlChan(id), func() {}, nil
}

func (b *memBus) deltaChan(id string) chan GatewayDelta {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := b.delta[id]
	if ch == nil {
		ch = make(chan GatewayDelta, 256)
		b.delta[id] = ch
	}
	return ch
}

func (b *memBus) SendDelta(_ context.Context, gatewayID string, d GatewayDelta) error {
	select {
	case b.deltaChan(gatewayID) <- d:
	default:
	}
	return nil
}

func (b *memBus) Deltas(_ context.Context, gatewayID string) (<-chan GatewayDelta, func(), error) {
	return b.deltaChan(gatewayID), func() {}, nil
}

func (b *memBus) feed(shardID string) *memFeed {
	b.mu.Lock()
	defer b.mu.Unlock()
	f := b.feeds[shardID]
	if f == nil {
		f = &memFeed{subs: map[string][]chan livequery.Change{}}
		b.feeds[shardID] = f
	}
	return f
}

// publishChange routes a change to the CURRENT owner shard's feed (the engine
// only matches it if that shard owns the partition).
func (b *memBus) publishChange(tenantID, entity string, c livequery.Change) {
	live, _ := b.LiveShards(context.Background())
	owner := Owner(PartitionOf(tenantID, entity), live)
	if owner == "" {
		return
	}
	b.feed(owner).publish(tenantID, entity, c)
}

// --- memFeed: a shard's engine feed.

type memFeed struct {
	mu   sync.Mutex
	subs map[string][]chan livequery.Change
}

func (f *memFeed) Changes(ctx context.Context, q livequery.LiveQuery, _ string) (<-chan livequery.Change, func(), error) {
	tid, _ := tenant.FromContext(ctx)
	key := tid + "|" + q.Entity
	ch := make(chan livequery.Change, 64)
	f.mu.Lock()
	f.subs[key] = append(f.subs[key], ch)
	f.mu.Unlock()
	cancel := func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		cs := f.subs[key]
		for i, c := range cs {
			if c == ch {
				f.subs[key] = append(cs[:i], cs[i+1:]...)
				break
			}
		}
	}
	return ch, cancel, nil
}

func (f *memFeed) publish(tenantID, entity string, c livequery.Change) {
	f.mu.Lock()
	chans := append([]chan livequery.Change(nil), f.subs[tenantID+"|"+entity]...)
	f.mu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- c:
		default:
		}
	}
}

// --- memStore: the shared "Postgres" — snapshot/refill oracle + registry.

type memStore struct {
	mu   sync.Mutex
	rows map[string]livequery.Row
	reg  map[string]livequery.Registration
}

func newMemStore() *memStore {
	return &memStore{rows: map[string]livequery.Row{}, reg: map[string]livequery.Registration{}}
}

// setRow inserts/updates a row in the shared truth (vals must include
// id + tenant_id).
func (m *memStore) setRow(id string, version int64, vals map[string]any) {
	raw, _ := json.Marshal(vals)
	m.mu.Lock()
	m.rows[id] = livequery.Row{AggID: id, Version: version, Vals: vals, Raw: raw}
	m.mu.Unlock()
}

func (m *memStore) Snapshot(ctx context.Context, q livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	tid, _ := tenant.FromContext(ctx)
	pred, _ := match.Compile(q.Where)
	m.mu.Lock()
	var rows []livequery.Row
	for _, r := range m.rows {
		if r.Vals["tenant_id"] != tid || !pred.Eval(r.Vals) {
			continue
		}
		rc := r
		rc.Cursor = livequery.SortKeyOf(r.Vals, q.Sort, r.AggID)
		rows = append(rows, rc)
	}
	m.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool {
		return livequery.CompareCursors(rows[i].Cursor, rows[j].Cursor, q.Sort) < 0
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (m *memStore) After(ctx context.Context, q livequery.LiveQuery, after livequery.Cursor, limit int) ([]livequery.Row, error) {
	all, _ := m.Snapshot(ctx, q, 0)
	var out []livequery.Row
	for _, r := range all {
		if livequery.CompareCursors(r.Cursor, after, q.Sort) > 0 {
			out = append(out, r)
			if limit > 0 && len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (m *memStore) Put(_ context.Context, r livequery.Registration) error {
	m.mu.Lock()
	m.reg[r.SubID] = r
	m.mu.Unlock()
	return nil
}
func (m *memStore) Delete(_ context.Context, subID string) error {
	m.mu.Lock()
	delete(m.reg, subID)
	m.mu.Unlock()
	return nil
}
func (m *memStore) ByPartition(_ context.Context, tenantID, entity string) ([]livequery.Registration, error) {
	return m.filterReg(func(r livequery.Registration) bool { return r.TenantID == tenantID && r.Entity == entity }), nil
}
func (m *memStore) ByPartitionNum(_ context.Context, p int) ([]livequery.Registration, error) {
	return m.filterReg(func(r livequery.Registration) bool { return r.Partition == p }), nil
}
func (m *memStore) ByGateway(_ context.Context, gw string) ([]livequery.Registration, error) {
	return m.filterReg(func(r livequery.Registration) bool { return r.GatewayID == gw }), nil
}
func (m *memStore) filterReg(keep func(livequery.Registration) bool) []livequery.Registration {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []livequery.Registration
	for _, r := range m.reg {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}
