package remotegrpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	"github.com/xraph/fabriq/remote"
)

// Client implements remote.Transport over a gRPC client connection. The caller
// owns dialing — including mTLS transport credentials — and hands the dialed
// connection here. Every call selects the bytes codec so the envelope crosses
// untouched.
type Client struct{ cc grpc.ClientConnInterface }

// NewClient wraps a dialed connection as a remote.Transport.
func NewClient(cc grpc.ClientConnInterface) *Client { return &Client{cc: cc} }

var _ remote.Transport = (*Client)(nil)

// Unary implements remote.Transport.
func (c *Client) Unary(ctx context.Context, method string, in []byte) ([]byte, error) {
	var out []byte
	if err := c.cc.Invoke(ctx, "/"+method, in, &out, grpc.CallContentSubtype(codecName)); err != nil {
		return nil, err
	}
	return out, nil
}

// ServerStream implements remote.Transport. The stream's ctx is made
// cancellable so Close stops it (the server-side handler unblocks on ctx.Done).
func (c *Client) ServerStream(ctx context.Context, method string, in []byte) (remote.Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	desc := &grpc.StreamDesc{StreamName: shortName(method), ServerStreams: true}
	cs, err := c.cc.NewStream(ctx, desc, "/"+method, grpc.CallContentSubtype(codecName))
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cs.SendMsg(in); err != nil {
		cancel()
		return nil, err
	}
	if err := cs.CloseSend(); err != nil {
		cancel()
		return nil, err
	}
	return &clientStream{cs: cs, cancel: cancel}, nil
}

type clientStream struct {
	cs     grpc.ClientStream
	cancel context.CancelFunc
}

// Recv returns the next frame, or io.EOF at a clean end (gRPC's RecvMsg
// surfaces io.EOF on stream completion, matching remote.Stream's contract).
func (s *clientStream) Recv() ([]byte, error) {
	var b []byte
	if err := s.cs.RecvMsg(&b); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *clientStream) Close() error {
	s.cancel()
	return nil
}

// ClientStream implements remote.Transport: the client Sends frames, then
// CloseAndRecv for the single response.
func (c *Client) ClientStream(ctx context.Context, method string) (remote.ClientStreamConn, error) {
	ctx, cancel := context.WithCancel(ctx)
	desc := &grpc.StreamDesc{StreamName: shortName(method), ClientStreams: true}
	cs, err := c.cc.NewStream(ctx, desc, "/"+method, grpc.CallContentSubtype(codecName))
	if err != nil {
		cancel()
		return nil, err
	}
	return &clientUploadStream{cs: cs, cancel: cancel}, nil
}

type clientUploadStream struct {
	cs     grpc.ClientStream
	cancel context.CancelFunc
}

func (c *clientUploadStream) Send(b []byte) error { return c.cs.SendMsg(b) }

func (c *clientUploadStream) CloseAndRecv() ([]byte, error) {
	if err := c.cs.CloseSend(); err != nil {
		return nil, err
	}
	var reply []byte
	if err := c.cs.RecvMsg(&reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *clientUploadStream) Close() error {
	c.cancel()
	return nil
}

// BidiStream implements remote.Transport: it opens a bidirectional gRPC stream
// (both ClientStreams and ServerStreams) whose Send and Recv are independent.
// Close cancels the stream, which the server-side handler observes on ctx.Done.
func (c *Client) BidiStream(ctx context.Context, method string) (remote.BidiStreamConn, error) {
	ctx, cancel := context.WithCancel(ctx)
	desc := &grpc.StreamDesc{StreamName: shortName(method), ClientStreams: true, ServerStreams: true}
	cs, err := c.cc.NewStream(ctx, desc, "/"+method, grpc.CallContentSubtype(codecName))
	if err != nil {
		cancel()
		return nil, err
	}
	return &clientBidiStream{cs: cs, cancel: cancel}, nil
}

type clientBidiStream struct {
	cs     grpc.ClientStream
	cancel context.CancelFunc
}

func (c *clientBidiStream) Send(b []byte) error { return c.cs.SendMsg(b) }

// Recv returns the next server frame, or io.EOF at a clean end (gRPC's RecvMsg
// surfaces io.EOF on stream completion, matching remote.BidiStreamConn).
func (c *clientBidiStream) Recv() ([]byte, error) {
	var b []byte
	if err := c.cs.RecvMsg(&b); err != nil {
		return nil, err
	}
	return b, nil
}

func (c *clientBidiStream) Close() error {
	c.cancel()
	return nil
}

// shortName maps a fully-qualified method ("pkg.Service/Method") to its stream
// name ("Method").
func shortName(method string) string {
	if i := strings.LastIndex(method, "/"); i >= 0 {
		return method[i+1:]
	}
	return method
}
