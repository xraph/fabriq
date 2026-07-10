package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// LiveSubscription is the control surface the server drives on a live
// subscription: tear it down (Close) or slide its window to a new anchor
// (Reanchor). The in-process *livequery.Handle satisfies it by construction, so
// a facade whose LiveQuery returns *livequery.Handle plugs in via the adapter in
// NewHandler; a test double can satisfy it directly.
type LiveSubscription interface {
	Reanchor(ctx context.Context, cursor *livequery.Cursor, limit int) (livequery.Snapshot, error)
	Close()
}

// LiveQuerier is the maintained-result-set surface the remote Live plane needs.
// It is NOT part of query.Fabric — LiveQuery lives on the concrete *fabriq.Fabriq
// — so the Handler adapts it from the facade (NewHandler); a facade without it
// makes the remote LiveQuery return ErrNotImplemented. The returned subscription
// carries Reanchor across the bidi stream.
type LiveQuerier interface {
	LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, LiveSubscription, error)
}

// concreteLiveQuerier is the facade shape (*fabriq.Fabriq): its LiveQuery
// returns the concrete *livequery.Handle. NewHandler adapts it to LiveQuerier.
type concreteLiveQuerier interface {
	LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, *livequery.Handle, error)
}

// liveQuerierAdapter widens a concrete facade (returning *livequery.Handle) to
// the LiveQuerier interface (returning LiveSubscription). *livequery.Handle
// already implements Reanchor+Close, so the adapter only re-types the return.
type liveQuerierAdapter struct{ c concreteLiveQuerier }

func (a liveQuerierAdapter) LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, LiveSubscription, error) {
	snap, deltas, handle, err := a.c.LiveQuery(ctx, q)
	if err != nil {
		return snap, deltas, nil, err
	}
	if handle == nil {
		// A configured facade always returns a non-nil handle on success; guard
		// so a nil concrete pointer never becomes a non-nil interface.
		return snap, deltas, nil, nil
	}
	return snap, deltas, handle, nil
}

// LiveHandle controls a remote maintained subscription over the bidirectional
// LiveQuery stream. It owns the single stream reader (deltas are demuxed to the
// caller's channel, reanchor snapshots to Reanchor's waiter) so Send and Recv
// never race across goroutines.
type LiveHandle struct {
	stream BidiStreamConn

	mu        sync.Mutex // serializes Reanchor: one outstanding control frame
	reanchor  chan reanchorReply
	closeOnce sync.Once
}

type reanchorReply struct {
	snap livequery.Snapshot
	err  error
}

// Close tears the remote subscription down: it closes the bidi stream, which the
// server observes (ctx.Done) and uses to Close the underlying engine handle.
func (h *LiveHandle) Close() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		if h.stream != nil {
			_ = h.stream.Close()
		}
	})
}

// Reanchor slides the maintained window to a new cursor anchor (and optionally a
// new size) mid-stream: it sends a Reanchor control frame on the bidi stream and
// blocks for the fresh snapshot the server returns, while deltas keep flowing on
// the delta channel. Concurrent Reanchor calls are serialized.
func (h *LiveHandle) Reanchor(ctx context.Context, cursor *livequery.Cursor, limit int) (livequery.Snapshot, error) {
	if h == nil || h.stream == nil {
		return livequery.Snapshot{}, ErrNotImplemented
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	var cursorJSON []byte
	if cursor != nil {
		var err error
		if cursorJSON, err = json.Marshal(cursor); err != nil {
			return livequery.Snapshot{}, fmt.Errorf("remote: marshal reanchor cursor: %w", err)
		}
	}
	frame, err := proto.Marshal(&fabriqpb.LiveClientFrame{
		Reanchor: &fabriqpb.LiveReanchor{Cursor: cursorJSON, Limit: int32(limit)}, //nolint:gosec // limit is a caller-supplied window size, always small and non-negative
	})
	if err != nil {
		return livequery.Snapshot{}, err
	}
	if err := h.stream.Send(frame); err != nil {
		return livequery.Snapshot{}, err
	}
	select {
	case rep, ok := <-h.reanchor:
		if !ok {
			return livequery.Snapshot{}, fmt.Errorf("remote: live stream closed during reanchor")
		}
		return rep.snap, rep.err
	case <-ctx.Done():
		return livequery.Snapshot{}, ctx.Err()
	}
}

// LiveQuery registers a maintained-result-set subscription over the remote
// transport's bidirectional Live stream: it returns the initial ordered
// snapshot, a channel of enter/leave/move/update deltas, and a handle to
// Reanchor or tear it down. It mirrors *fabriq.Fabriq.LiveQuery. Close the
// handle — or cancel ctx — to end the subscription.
func (r *Fabric) LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, *LiveHandle, error) {
	body, err := json.Marshal(q)
	if err != nil {
		return livequery.Snapshot{}, nil, nil, err
	}
	queryFrame, err := proto.Marshal(&fabriqpb.LiveClientFrame{
		Query: &fabriqpb.LiveQueryRequest{Query: body},
	})
	if err != nil {
		return livequery.Snapshot{}, nil, nil, err
	}
	stream, err := r.t.BidiStream(ctx, MethodLiveQuery)
	if err != nil {
		return livequery.Snapshot{}, nil, nil, err
	}
	if err = stream.Send(queryFrame); err != nil {
		_ = stream.Close()
		return livequery.Snapshot{}, nil, nil, err
	}
	// First frame: the snapshot, or a setup error (validation / authz / not
	// configured) returned synchronously like the in-process contract.
	first, err := stream.Recv()
	if err != nil {
		_ = stream.Close()
		return livequery.Snapshot{}, nil, nil, err
	}
	var hs fabriqpb.LiveFrame
	if err := proto.Unmarshal(first, &hs); err != nil {
		_ = stream.Close()
		return livequery.Snapshot{}, nil, nil, fmt.Errorf("remote: decode live snapshot frame: %w", err)
	}
	if hs.Error != nil {
		_ = stream.Close()
		return livequery.Snapshot{}, nil, nil, errorFromProto(hs.Error)
	}
	var snap livequery.Snapshot
	if len(hs.Snapshot) > 0 {
		if err := json.Unmarshal(hs.Snapshot, &snap); err != nil {
			_ = stream.Close()
			return livequery.Snapshot{}, nil, nil, fmt.Errorf("remote: decode snapshot: %w", err)
		}
	}
	handle := &LiveHandle{stream: stream, reanchor: make(chan reanchorReply, 1)}
	out := make(chan livequery.LiveDelta)
	// buf decouples the single stream reader from the caller draining `out`. The
	// reader must never block on `out`: a Reanchor reply arrives on the wire
	// AFTER any buffered deltas, so if the caller calls Reanchor before draining
	// deltas, a reader blocked on `out` could never surface the reply (deadlock).
	// The reader appends deltas to an unbounded buffer and signals a pump that
	// feeds `out` at the caller's pace.
	buf := &deltaBuffer{signal: make(chan struct{}, 1)}
	go buf.pump(ctx, out)
	go func() {
		defer stream.Close()
		defer buf.close()
		// The reanchor channel is closed once the reader exits so a Reanchor
		// blocked on a reply (server gone mid-reanchor) unblocks with an error.
		defer close(handle.reanchor)
		for {
			frame, rerr := stream.Recv()
			if rerr != nil {
				return // io.EOF (clean end) or transport error
			}
			var lf fabriqpb.LiveFrame
			if proto.Unmarshal(frame, &lf) != nil {
				continue
			}
			switch {
			case lf.Snapshot != nil || lf.Error != nil:
				// A reanchor reply: fresh snapshot (or a reanchor error).
				var rep reanchorReply
				if lf.Error != nil {
					rep.err = errorFromProto(lf.Error)
				} else if len(lf.Snapshot) > 0 {
					if uerr := json.Unmarshal(lf.Snapshot, &rep.snap); uerr != nil {
						rep.err = fmt.Errorf("remote: decode reanchor snapshot: %w", uerr)
					}
				}
				select {
				case handle.reanchor <- rep:
				case <-ctx.Done():
					return
				}
			case len(lf.Delta) > 0:
				var d livequery.LiveDelta
				if json.Unmarshal(lf.Delta, &d) != nil {
					continue
				}
				buf.push(d)
			}
		}
	}()
	return snap, out, handle, nil
}

// deltaBuffer is an unbounded FIFO between the stream reader (push, never
// blocks) and the caller's delta channel (drained by pump at the caller's
// pace). Decoupling them is what lets a Reanchor reply — which the server sends
// after in-flight deltas — reach the caller even when it calls Reanchor before
// draining deltas.
type deltaBuffer struct {
	mu     sync.Mutex
	items  []livequery.LiveDelta
	closed bool
	signal chan struct{}
}

func (b *deltaBuffer) push(d livequery.LiveDelta) {
	b.mu.Lock()
	b.items = append(b.items, d)
	b.mu.Unlock()
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *deltaBuffer) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *deltaBuffer) pump(ctx context.Context, out chan<- livequery.LiveDelta) {
	defer close(out)
	for {
		b.mu.Lock()
		if len(b.items) == 0 {
			closed := b.closed
			b.mu.Unlock()
			if closed {
				return
			}
			select {
			case <-b.signal:
			case <-ctx.Done():
				return
			}
			continue
		}
		d := b.items[0]
		b.items = b.items[1:]
		b.mu.Unlock()
		select {
		case out <- d:
		case <-ctx.Done():
			return
		}
	}
}
