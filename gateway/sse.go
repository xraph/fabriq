package gateway

import (
	"context"
	"time"
)

// SSESink is the minimal Server-Sent Events transport ServeSSE writes to.
// core/subscribe.SSEWriter satisfies it directly; tests substitute a fake.
type SSESink interface {
	// WriteEvent emits one event: id = SSE "id:" (resume position),
	// event = the delta op name, v = the JSON payload. It flushes.
	WriteEvent(id, event string, v any) error
	// SetWriteDeadline bounds how long one write may block (backpressure).
	SetWriteDeadline(time.Time) error
	// Heartbeat writes a keep-alive comment.
	Heartbeat() error
}

// SSEOptions tune the SSE delivery loop.
type SSEOptions struct {
	// HeartbeatInterval is how often a keep-alive comment is sent on an idle
	// stream. Defaults to 15s.
	HeartbeatInterval time.Duration
	// WriteTimeout bounds a single event write; on a stalled client the write
	// fails and the connection is torn down (the client reconnects to a fresh
	// snapshot). Zero disables the deadline.
	WriteTimeout time.Duration
}

// ServeSSE forwards a subscription's snapshot+delta stream onto an SSE sink
// until the stream closes or ctx is cancelled. The snapshot arrives folded into
// the stream as OpReset + OpEnter rows, so there is one uniform path: every
// delta is written as an event named for its op, with id = StreamID.
//
// It returns nil on a clean end (client disconnect via ctx, or the backend
// closing the stream) and the write error on a failed/stalled write — which the
// caller turns into a connection teardown.
func ServeSSE(ctx context.Context, sink SSESink, sub *Sub, opts SSEOptions) error {
	hb := opts.HeartbeatInterval
	if hb <= 0 {
		hb = 15 * time.Second
	}
	ticker := time.NewTicker(hb)
	defer ticker.Stop()

	arm := func() {
		if opts.WriteTimeout > 0 {
			_ = sink.SetWriteDeadline(time.Now().Add(opts.WriteTimeout))
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-sub.Deltas:
			if !ok {
				return nil
			}
			arm()
			if err := sink.WriteEvent(d.StreamID, d.Op.String(), frameOf(d)); err != nil {
				return err
			}
		case <-ticker.C:
			arm()
			if err := sink.Heartbeat(); err != nil {
				return err
			}
		}
	}
}
