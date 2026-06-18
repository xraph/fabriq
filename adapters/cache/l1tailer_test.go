package cache

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/event"
)

// fakeTailer delivers a fixed set of envelopes then returns.
type fakeTailer struct{ envs []event.Envelope }

func (f *fakeTailer) TailEvents(_ context.Context, handle func(event.Envelope) error) error {
	for _, e := range f.envs {
		if err := handle(e); err != nil {
			return err
		}
	}
	return nil
}

func TestRunL1EvictTailer_EvictsPerEnvelope(t *testing.T) {
	m := newMem()
	l := NewL1(m, l1reg(t), 64, time.Minute)
	// Pre-warm an asset row + query entry under tenant acme.
	ctx := l1ctx(t)
	_ = l.Set(ctx, rowKS(), "a1", []byte("v"))
	_ = l.Set(ctx, queryKS(), "fp1", []byte("ids"))

	ft := &fakeTailer{envs: []event.Envelope{
		{TenantID: "acme", Aggregate: "asset", AggID: "a1"},
	}}
	if err := RunL1EvictTailer(context.Background(), ft, l); err != nil {
		t.Fatal(err)
	}
	// After EvictLocal, L1 entries are orphaned/deleted. Since the fake inner
	// (memCache) still holds the data, a plain Get falls through to inner and
	// returns the value — so we assert via inner call-count increasing, not via
	// a "miss" result.
	gets := m.gets
	_, _, _ = l.Get(ctx, rowKS(), "a1")
	_, _, _ = l.Get(ctx, queryKS(), "fp1")
	if m.gets != gets+2 {
		t.Fatalf("after tailer both Gets must fall through to inner (gets %d->%d, want +2)", gets, m.gets)
	}
}
