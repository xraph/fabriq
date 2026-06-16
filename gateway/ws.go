package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WSConn is the minimal WebSocket transport ServeWS uses. forge.Connection
// satisfies it directly (it is a subset of that interface); tests substitute an
// in-memory fake.
type WSConn interface {
	// ReadJSON decodes the next client command frame; it errors when the client
	// disconnects or the connection is closed.
	ReadJSON(v any) error
	// WriteJSON sends one server frame to the client.
	WriteJSON(v any) error
	// Close tears the socket down (also used to abort a stalled write).
	Close() error
	// Context is cancelled when the connection ends.
	Context() context.Context
}

// WSOptions tune the WebSocket loop.
type WSOptions struct {
	// WriteTimeout bounds a single frame write. forge.Connection exposes no
	// write deadline, so the bound is enforced with a watchdog: a write that
	// stalls past WriteTimeout closes the connection (the client reconnects to a
	// fresh snapshot). Zero disables the watchdog (the established idiom: rely on
	// the bounded delta buffer and tear down on any write error).
	WriteTimeout time.Duration
}

var errWriteTimeout = errors.New("gateway: websocket write timed out")

// ServeWS runs one WebSocket subscription lifecycle: it reads the initial
// subscribe command, opens the backend subscription, then pumps deltas to the
// client (write pump) while serving reanchor/unsubscribe commands (read pump)
// until either side ends. A clean unsubscribe or backend stream-close returns
// nil; a transport error is returned for logging. The subscription is always
// torn down on exit.
func ServeWS(ctx context.Context, conn WSConn, backend Backend, opts WSOptions) error {
	var first ClientCommand
	if err := conn.ReadJSON(&first); err != nil {
		return err
	}
	if first.Action != ActionSubscribe || first.Query == nil {
		return fmt.Errorf("gateway: first websocket message must be a subscribe carrying a query")
	}

	sub, err := backend.Subscribe(ctx, *first.Query)
	if err != nil {
		return err
	}
	defer sub.Close()

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	var writeErr error
	go func() {
		writeErr = wsWritePump(pctx, conn, sub, opts)
		_ = conn.Close() // unblock the read pump when the backend ends the stream
		close(done)
	}()

	readErr := wsReadPump(conn, sub) // blocks until a client error or unsubscribe
	cancel()                         // stop the write pump
	<-done

	if readErr != nil {
		return readErr
	}
	return writeErr
}

// wsReadPump serves client→server commands until the client disconnects (read
// error) or unsubscribes (clean nil return).
func wsReadPump(conn WSConn, sub *Sub) error {
	for {
		var cmd ClientCommand
		if err := conn.ReadJSON(&cmd); err != nil {
			return err
		}
		switch cmd.Action {
		case ActionReanchor:
			_ = sub.Reanchor(conn.Context(), cmd.Cursor, cmd.Limit)
		case ActionUnsubscribe:
			return nil
		default:
			// Ignore unknown actions and a duplicate subscribe — the connection
			// already has its subscription.
		}
	}
}

// wsWritePump forwards the subscription's snapshot+delta stream to the client
// until the stream closes or ctx is cancelled.
func wsWritePump(ctx context.Context, conn WSConn, sub *Sub, opts WSOptions) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-sub.Deltas:
			if !ok {
				return nil
			}
			if err := wsWrite(ctx, conn, frameOf(d), opts.WriteTimeout); err != nil {
				return err
			}
		}
	}
}

// wsWrite writes one frame, bounding the write with a watchdog when a timeout is
// configured. A stalled write closes the connection so the goroutine cannot wedge.
func wsWrite(ctx context.Context, conn WSConn, f Frame, timeout time.Duration) error {
	if timeout <= 0 {
		return conn.WriteJSON(f)
	}
	res := make(chan error, 1) // buffered: the writer goroutine never blocks, even after timeout
	go func() { res <- conn.WriteJSON(f) }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-res:
		return err
	case <-timer.C:
		_ = conn.Close() // abort the stalled write; the goroutine then unblocks and exits
		return errWriteTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}
