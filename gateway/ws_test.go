package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
)

// fakeConn is an in-memory WSConn: the test feeds inbound command frames and
// reads back recorded outbound frames.
type fakeConn struct {
	in      chan []byte
	closeCh chan struct{}
	ctx     context.Context

	mu       sync.Mutex
	out      []Frame
	closed   bool
	block    chan struct{} // if set, WriteJSON blocks on it (watchdog test)
	writeErr error
}

func newFakeConn() *fakeConn {
	return &fakeConn{in: make(chan []byte, 8), closeCh: make(chan struct{}), ctx: context.Background()}
}

func (c *fakeConn) feed(cmd ClientCommand) {
	raw, _ := json.Marshal(cmd)
	c.in <- raw
}

func (c *fakeConn) ReadJSON(v any) error {
	select {
	case raw, ok := <-c.in:
		if !ok {
			return errors.New("closed")
		}
		return json.Unmarshal(raw, v)
	case <-c.closeCh:
		return errors.New("closed")
	}
}

func (c *fakeConn) WriteJSON(v any) error {
	c.mu.Lock()
	we, block := c.writeErr, c.block
	c.mu.Unlock()
	if we != nil {
		return we
	}
	if block != nil {
		select {
		case <-block:
		case <-c.closeCh:
			return errors.New("closed")
		}
	}
	f, ok := v.(Frame)
	if !ok {
		return errors.New("ws write: not a Frame")
	}
	c.mu.Lock()
	c.out = append(c.out, f)
	c.mu.Unlock()
	return nil
}

func (c *fakeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.closeCh)
	}
	return nil
}

func (c *fakeConn) Context() context.Context { return c.ctx }

func (c *fakeConn) frames() []Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Frame(nil), c.out...)
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type reanchorCall struct {
	cursor *livequery.Cursor
	limit  int
}

type fakeBackend struct {
	deltas     chan livequery.LiveDelta
	subscribed chan livequery.LiveQuery
	reanchors  chan reanchorCall
	closed     chan struct{}
	closeOnce  sync.Once
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		deltas:     make(chan livequery.LiveDelta, 8),
		subscribed: make(chan livequery.LiveQuery, 1),
		reanchors:  make(chan reanchorCall, 4),
		closed:     make(chan struct{}),
	}
}

func (b *fakeBackend) Subscribe(_ context.Context, q livequery.LiveQuery) (*Sub, error) {
	b.subscribed <- q
	reanchor := func(_ context.Context, cur *livequery.Cursor, limit int) error {
		b.reanchors <- reanchorCall{cur, limit}
		return nil
	}
	closeFn := func() { b.closeOnce.Do(func() { close(b.closed) }) }
	return NewSub("sub-1", b.deltas, reanchor, closeFn), nil
}

func subscribeCmd(entity string) ClientCommand {
	return ClientCommand{Action: ActionSubscribe, Query: &livequery.LiveQuery{Entity: entity}}
}

func TestServeWS_SubscribeThenForwardDelta(t *testing.T) {
	conn := newFakeConn()
	be := newFakeBackend()
	done := make(chan error, 1)
	go func() { done <- ServeWS(context.Background(), conn, be, WSOptions{}) }()

	conn.feed(subscribeCmd("asset"))
	select {
	case q := <-be.subscribed:
		if q.Entity != "asset" {
			t.Fatalf("subscribed entity = %q", q.Entity)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe was not called")
	}

	be.deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a", NewIndex: 0}
	waitFor(t, func() bool { return len(conn.frames()) == 1 })
	if got := conn.frames()[0]; got.Op != "enter" || got.AggID != "a" {
		t.Fatalf("frame = %+v", got)
	}

	conn.Close() // client disconnects
	if err := <-done; err == nil {
		t.Log("ServeWS returned nil on disconnect (acceptable)")
	}
	select {
	case <-be.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription was not closed on disconnect")
	}
}

func TestServeWS_ReanchorCommand(t *testing.T) {
	conn := newFakeConn()
	be := newFakeBackend()
	go ServeWS(context.Background(), conn, be, WSOptions{})

	conn.feed(subscribeCmd("asset"))
	<-be.subscribed
	conn.feed(ClientCommand{Action: ActionReanchor, Cursor: &livequery.Cursor{Values: []any{"z"}}, Limit: 25})

	select {
	case rc := <-be.reanchors:
		if rc.limit != 25 || rc.cursor == nil || len(rc.cursor.Values) != 1 {
			t.Fatalf("reanchor call = %+v", rc)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reanchor was not called")
	}
	conn.Close()
}

func TestServeWS_UnsubscribeEndsCleanly(t *testing.T) {
	conn := newFakeConn()
	be := newFakeBackend()
	done := make(chan error, 1)
	go func() { done <- ServeWS(context.Background(), conn, be, WSOptions{}) }()

	conn.feed(subscribeCmd("asset"))
	<-be.subscribed
	conn.feed(ClientCommand{Action: ActionUnsubscribe})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeWS = %v, want nil on clean unsubscribe", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeWS did not return on unsubscribe")
	}
	select {
	case <-be.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription not closed after unsubscribe")
	}
}

func TestServeWS_FirstMessageMustBeSubscribe(t *testing.T) {
	conn := newFakeConn()
	be := newFakeBackend()
	done := make(chan error, 1)
	go func() { done <- ServeWS(context.Background(), conn, be, WSOptions{}) }()

	conn.feed(ClientCommand{Action: ActionReanchor})
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ServeWS = nil, want error when first message is not subscribe")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeWS did not reject a non-subscribe first message")
	}
}

func TestServeWS_StreamCloseEndsConnection(t *testing.T) {
	conn := newFakeConn()
	be := newFakeBackend()
	done := make(chan error, 1)
	go func() { done <- ServeWS(context.Background(), conn, be, WSOptions{}) }()

	conn.feed(subscribeCmd("asset"))
	<-be.subscribed
	close(be.deltas) // backend ends the stream

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeWS did not return when the delta stream closed")
	}
	if !conn.isClosed() {
		t.Fatal("conn should be closed when the stream ends")
	}
}

func TestServeWS_WriteWatchdogTearsDownStalledClient(t *testing.T) {
	conn := newFakeConn()
	conn.block = make(chan struct{}) // WriteJSON will block forever
	be := newFakeBackend()
	done := make(chan error, 1)
	go func() {
		done <- ServeWS(context.Background(), conn, be, WSOptions{WriteTimeout: 20 * time.Millisecond})
	}()

	conn.feed(subscribeCmd("asset"))
	<-be.subscribed
	be.deltas <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a"}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeWS did not tear down a stalled client")
	}
	if !conn.isClosed() {
		t.Fatal("watchdog should have closed the stalled conn")
	}
}
