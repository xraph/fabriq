package falkordb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPingReadinessDuringClose(t *testing.T) {
	socket, backend := net.Pipe()
	conn := &heldCloseConn{
		Conn:    socket,
		closing: make(chan struct{}),
		release: make(chan struct{}),
	}
	releaseClose := sync.OnceFunc(func() { close(conn.release) })
	var workers sync.WaitGroup
	var dials atomic.Int32
	client := redis.NewClient(&redis.Options{
		Protocol:        2,
		DisableIdentity: true,
		MaxRetries:      -1,
		ReadTimeout:     time.Second,
		WriteTimeout:    time.Second,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			if dials.Add(1) != 1 {
				return nil, errors.New("unexpected readiness dial during close")
			}
			return conn, nil
		},
	})
	readinessCtx, cancelReadiness := context.WithCancel(context.Background())
	adapter := &Adapter{client: client, readinessCtx: readinessCtx, cancelReadiness: cancelReadiness}
	t.Cleanup(func() {
		releaseClose()
		_ = backend.Close()
		_ = socket.Close()
		workers.Wait()
		_ = adapter.Close()
	})

	backendErr := make(chan error, 1)
	workers.Go(func() { backendErr <- serveSharedReadinessConn(backend) })
	if err := adapter.Ping(t.Context()); err != nil {
		t.Fatalf("open shared Redis connection: %v", err)
	}
	if err := <-backendErr; err != nil {
		t.Fatalf("serve shared Redis connection: %v", err)
	}
	conn.hold.Store(true)
	closeErr := make(chan error, 1)
	workers.Go(func() { closeErr <- adapter.Close() })
	select {
	case <-conn.closing:
	case <-time.After(time.Second):
		t.Fatal("adapter did not begin closing the shared Redis connection")
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	probeErr := make(chan error, 1)
	workers.Go(func() { probeErr <- adapter.PingReadiness(ctx) })
	select {
	case err := <-probeErr:
		if !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("readiness during Close = %v, want redis.ErrClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("readiness waited for shared Redis close past the caller deadline")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("shared and readiness dials = %d, want only the original shared dial", got)
	}

	releaseClose()
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("close adapter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not finish closing the shared Redis connection")
	}
}

type heldCloseConn struct {
	net.Conn
	hold    atomic.Bool
	once    sync.Once
	closing chan struct{}
	release chan struct{}
}

func (c *heldCloseConn) Close() error {
	if c.hold.Load() {
		c.once.Do(func() { close(c.closing) })
		<-c.release
	}
	return c.Conn.Close()
}

func serveSharedReadinessConn(conn net.Conn) error {
	for _, step := range []struct {
		command string
		reply   string
	}{
		{"*2\r\n$5\r\nhello\r\n$1\r\n2\r\n", "-ERR unknown command\r\n"},
		{"*1\r\n$4\r\nping\r\n", "+PONG\r\n"},
	} {
		command := make([]byte, len(step.command))
		if _, err := io.ReadFull(conn, command); err != nil {
			return err
		}
		if string(command) != step.command {
			return fmt.Errorf("shared Redis command = %q, want %q", command, step.command)
		}
		if _, err := io.WriteString(conn, step.reply); err != nil {
			return err
		}
	}
	return nil
}
