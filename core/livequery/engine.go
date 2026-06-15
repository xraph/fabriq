package livequery

import (
	"context"

	"github.com/xraph/fabriq/core/livequery/match"
)

// Feed is the change source for a live query (P1: a fake / the in-process
// Redis tail bridged to Change; P3: per-partition consumer groups).
type Feed interface {
	Changes(ctx context.Context, q LiveQuery, fromWatermark string) (<-chan Change, func(), error)
}

// EngineOptions tunes window cushion and the per-subscription delta buffer.
type EngineOptions struct {
	Cushion int
	Buffer  int
}

// Engine runs single-node Maintained live queries: snapshot from the
// Snapshotter, then fold Feed changes through a Window into a delta channel.
type Engine struct {
	snap   Snapshotter
	refill Refiller
	feed   Feed
	opts   EngineOptions
}

// NewEngine wires the engine; zero options fall back to sane defaults.
func NewEngine(snap Snapshotter, refill Refiller, feed Feed, opts EngineOptions) *Engine {
	if opts.Cushion <= 0 {
		opts.Cushion = 2
	}
	if opts.Buffer <= 0 {
		opts.Buffer = 64
	}
	return &Engine{snap: snap, refill: refill, feed: feed, opts: opts}
}

// Subscribe returns the initial Snapshot and a live LiveDelta channel. The
// channel closes when ctx is cancelled or the feed ends. Gapless handoff:
// the feed is opened (from "now") BEFORE the snapshot, changes are buffered,
// and only versions newer than the seeded row are applied.
func (e *Engine) Subscribe(ctx context.Context, q LiveQuery) (Snapshot, <-chan LiveDelta, func(), error) {
	cctx, cancel := context.WithCancel(ctx)

	changes, feedCancel, err := e.feed.Changes(cctx, q, "$")
	if err != nil {
		cancel()
		return Snapshot{}, nil, nil, err
	}

	seed, err := e.snap.Snapshot(cctx, q, q.Limit+e.opts.Cushion)
	if err != nil {
		feedCancel()
		cancel()
		return Snapshot{}, nil, nil, err
	}
	complete := len(seed) < q.Limit+e.opts.Cushion

	pred, err := match.Compile(q.Where)
	if err != nil {
		feedCancel()
		cancel()
		return Snapshot{}, nil, nil, err
	}
	win, err := NewWindow(q, pred, seed, complete, e.opts.Cushion, e.refill)
	if err != nil {
		feedCancel()
		cancel()
		return Snapshot{}, nil, nil, err
	}

	snap := Snapshot{Rows: win.Visible()}

	out := make(chan LiveDelta, e.opts.Buffer)
	go func() {
		defer close(out)
		defer feedCancel()
		seen := map[string]int64{}
		for _, r := range seed {
			seen[r.AggID] = r.Version
		}
		for {
			select {
			case <-cctx.Done():
				return
			case ch, ok := <-changes:
				if !ok {
					return
				}
				if v, dup := seen[ch.AggID]; dup && ch.Version <= v {
					continue // already reflected by the snapshot (gapless dedup)
				}
				seen[ch.AggID] = ch.Version
				for _, d := range win.Apply(cctx, ch) {
					d.StreamID = ch.StreamID
					d.At = ch.At
					select {
					case out <- d:
					case <-cctx.Done():
						return
					}
				}
			}
		}
	}()

	return snap, out, cancel, nil
}
