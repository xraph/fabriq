package remote

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/remote/fabriqpb"
)

// LiveQuerier is the maintained-result-set surface the remote Live plane needs.
// It is NOT part of query.Fabric — LiveQuery lives on the concrete *fabriq.Fabriq
// — so the Handler type-asserts it from the facade (NewHandler); a facade
// without it makes the remote LiveQuery return ErrNotImplemented. The signature
// matches *fabriq.Fabriq exactly, so the facade satisfies this by construction.
type LiveQuerier interface {
	LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, *livequery.Handle, error)
}

// LiveHandle controls a remote maintained subscription. Unlike the in-process
// *livequery.Handle it cannot carry engine state across the wire, so it exposes
// only Close (tear down); Reanchor (deep scroll) needs a bidirectional stream
// and is a follow-on (ADR 0009).
type LiveHandle struct {
	stream Stream
}

// Close tears the remote subscription down: it cancels the stream, which the
// server observes (ctx.Done) and uses to Close the underlying engine handle.
func (h *LiveHandle) Close() {
	if h != nil && h.stream != nil {
		_ = h.stream.Close()
	}
}

// Reanchor slides a maintained window to a new cursor anchor. It needs the
// bidirectional Live stream (client→server control mid-stream), not yet wired,
// so it returns ErrNotImplemented.
func (h *LiveHandle) Reanchor(context.Context, *livequery.Cursor, int) (livequery.Snapshot, error) {
	return livequery.Snapshot{}, ErrNotImplemented
}

// LiveQuery registers a maintained-result-set subscription over the remote
// transport: it returns the initial ordered snapshot, a channel of
// enter/leave/move/update deltas, and a handle to tear it down. It mirrors
// *fabriq.Fabriq.LiveQuery, except the handle is a remote *LiveHandle (no
// Reanchor yet). Close the handle — or cancel ctx — to end the subscription.
func (r *RemoteFabric) LiveQuery(ctx context.Context, q livequery.LiveQuery) (livequery.Snapshot, <-chan livequery.LiveDelta, *LiveHandle, error) {
	body, err := json.Marshal(q)
	if err != nil {
		return livequery.Snapshot{}, nil, nil, err
	}
	in, err := proto.Marshal(&fabriqpb.LiveQueryRequest{Query: body})
	if err != nil {
		return livequery.Snapshot{}, nil, nil, err
	}
	stream, err := r.t.ServerStream(ctx, MethodLiveQuery, in)
	if err != nil {
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
	out := make(chan livequery.LiveDelta)
	go func() {
		defer close(out)
		defer stream.Close()
		for {
			frame, rerr := stream.Recv()
			if rerr != nil {
				return // io.EOF (clean end) or transport error
			}
			var lf fabriqpb.LiveFrame
			if proto.Unmarshal(frame, &lf) != nil || len(lf.Delta) == 0 {
				continue
			}
			var d livequery.LiveDelta
			if json.Unmarshal(lf.Delta, &d) != nil {
				continue
			}
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
	}()
	return snap, out, &LiveHandle{stream: stream}, nil
}
