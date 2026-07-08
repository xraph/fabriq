package remote

import (
	"context"
	"io"
	"sync"
)

// Transport is the codec- and wire-neutral seam the client and server halves
// sit on. The production binding is gRPC over HTTP/2 with protobuf framing and
// mTLS (see proto/fabriq/v1/fabriq.proto and ADR 0009); Loopback is the
// in-process binding used to exercise the envelope without a network.
//
// The ctx passed here carries the tenant/principal; the gRPC Transport turns
// those into call metadata that the server edge authenticates. in/out are the
// marshaled envelope bytes — canonical JSON in this skeleton, protobuf once the
// stubs are generated; the planes above this seam do not care which.
type Transport interface {
	// Unary invokes a request/response method.
	Unary(ctx context.Context, method string, in []byte) (out []byte, err error)
	// ServerStream opens a server-streaming method: one request, a stream of
	// framed responses. Drain Recv until io.EOF (clean end) or a non-EOF error,
	// then Close to release it.
	ServerStream(ctx context.Context, method string, in []byte) (Stream, error)
	// ClientStream opens a client-streaming method: the client Sends N frames
	// then CloseAndRecv for the single response. Used by chunked blob upload.
	ClientStream(ctx context.Context, method string) (ClientStreamConn, error)
}

// Stream is the client view of a server-streaming response.
type Stream interface {
	// Recv returns the next frame, (nil, io.EOF) at a clean end, or (nil, err).
	Recv() ([]byte, error)
	// Close releases the stream and signals the server to stop producing.
	Close() error
}

// ClientStreamConn is the client view of a client-streaming call: send N frames,
// then CloseAndRecv for the single response.
type ClientStreamConn interface {
	Send(frame []byte) error
	CloseAndRecv() (reply []byte, err error)
	Close() error
}

// Fully-qualified RPC method names, mirroring the proto service.
const (
	MethodExec               = "fabriq.v1.Fabriq/Exec"
	MethodExecBatch          = "fabriq.v1.Fabriq/ExecBatch"
	MethodGet                = "fabriq.v1.Fabriq/Get"
	MethodGetMany            = "fabriq.v1.Fabriq/GetMany"
	MethodList               = "fabriq.v1.Fabriq/List"
	MethodSubscribe          = "fabriq.v1.Fabriq/Subscribe"
	MethodLiveQuery          = "fabriq.v1.Fabriq/LiveQuery"
	MethodPutBlob            = "fabriq.v1.Fabriq/PutBlob"
	MethodGetBlob            = "fabriq.v1.Fabriq/GetBlob"
	MethodHeadBlob           = "fabriq.v1.Fabriq/HeadBlob"
	MethodDeleteBlob         = "fabriq.v1.Fabriq/DeleteBlob"
	MethodPresignBlob        = "fabriq.v1.Fabriq/PresignBlob"
	MethodVectorSimilar      = "fabriq.v1.Fabriq/VectorSimilar"
	MethodVectorUpsert       = "fabriq.v1.Fabriq/VectorUpsert"
	MethodVectorDelete       = "fabriq.v1.Fabriq/VectorDelete"
	MethodVectorDeleteByMeta = "fabriq.v1.Fabriq/VectorDeleteByMeta"
	MethodVectorGet          = "fabriq.v1.Fabriq/VectorGet"
	MethodSearch             = "fabriq.v1.Fabriq/Search"
	MethodGraphQuery         = "fabriq.v1.Fabriq/GraphQuery"
	MethodTSBulkWrite        = "fabriq.v1.Fabriq/TSBulkWrite"
	MethodTSRange            = "fabriq.v1.Fabriq/TSRange"
	MethodSpatialUpsert      = "fabriq.v1.Fabriq/SpatialUpsert"
	MethodSpatialWithin      = "fabriq.v1.Fabriq/SpatialWithin"
	MethodSpatialGet         = "fabriq.v1.Fabriq/SpatialGet"
	MethodSpatialDelete      = "fabriq.v1.Fabriq/SpatialDelete"
)

// Loopback is an in-process Transport that dispatches straight to a Handler —
// no network, only the envelope (de)serialization itself. It exists so the
// client, the envelope and the server-side handlers can be round-tripped in a
// unit test before the gRPC binding exists.
type Loopback struct{ Handler *Handler }

// Unary implements Transport.
func (l Loopback) Unary(ctx context.Context, method string, in []byte) ([]byte, error) {
	return l.Handler.Dispatch(ctx, method, in)
}

// ServerStream implements Transport: it runs the streaming handler in a
// goroutine that pushes frames into a buffered channel the returned Stream
// drains. The ctx is made cancellable so Close (or client disconnect) stops the
// server goroutine instead of leaking it — every send is guarded by ctx.Done.
func (l Loopback) ServerStream(ctx context.Context, method string, in []byte) (Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &loopbackStream{frames: make(chan []byte, 16), cancel: cancel}
	go func() {
		defer close(s.frames)
		send := func(b []byte) error {
			select {
			case s.frames <- b:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		s.setErr(l.Handler.DispatchStream(ctx, method, in, send))
	}()
	return s, nil
}

type loopbackStream struct {
	frames chan []byte
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

func (s *loopbackStream) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *loopbackStream) Recv() ([]byte, error) {
	b, ok := <-s.frames
	if !ok {
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	return b, nil
}

func (s *loopbackStream) Close() error {
	s.cancel()
	return nil
}

// ClientStream implements Transport: it runs the client-streaming handler in a
// goroutine that pulls frames from a channel the caller Sends to and produces a
// single reply. The ctx is cancelled once the handler returns so a pending Send
// unblocks instead of leaking.
func (l Loopback) ClientStream(ctx context.Context, method string) (ClientStreamConn, error) {
	ctx, cancel := context.WithCancel(ctx)
	c := &loopbackClientStream{
		frames:  make(chan []byte, 16),
		replyCh: make(chan clientStreamResult, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	go func() {
		recv := func() ([]byte, error) {
			select {
			case f, ok := <-c.frames:
				if !ok {
					return nil, io.EOF
				}
				return f, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		reply, err := l.Handler.DispatchClientStream(ctx, method, recv)
		c.replyCh <- clientStreamResult{reply: reply, err: err}
		cancel()
	}()
	return c, nil
}

type clientStreamResult struct {
	reply []byte
	err   error
}

type loopbackClientStream struct {
	frames  chan []byte
	replyCh chan clientStreamResult
	ctx     context.Context
	cancel  context.CancelFunc

	mu     sync.Mutex
	closed bool
}

func (c *loopbackClientStream) Send(b []byte) error {
	frame := append([]byte(nil), b...) // copy: the caller may reuse the buffer
	select {
	case c.frames <- frame:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func (c *loopbackClientStream) CloseAndRecv() ([]byte, error) {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.frames)
	}
	c.mu.Unlock()
	res := <-c.replyCh
	return res.reply, res.err
}

func (c *loopbackClientStream) Close() error {
	c.cancel()
	return nil
}
