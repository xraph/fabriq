package postgres

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/grove/driver"
)

// fakeDedicatedConn models grove's dedicated connection. A real pgx
// connection is NOT safe for concurrent use; this fake makes that contract
// observable: if QueryRow is entered while the connection is already in use
// (i.e. by another goroutine), it records an overlap. inUse is held from
// QueryRow until the returned Row's Scan completes, mirroring how pgx locks
// the connection across the query/result lifecycle.
type fakeDedicatedConn struct {
	inUse   atomic.Bool
	overlap atomic.Bool
	hook    func(query string)
}

func (c *fakeDedicatedConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	if !c.inUse.CompareAndSwap(false, true) {
		c.overlap.Store(true)
	}
	if c.hook != nil {
		c.hook(query)
	}
	return &fakeRow{conn: c}
}

func (c *fakeDedicatedConn) Exec(context.Context, string, ...any) (driver.Result, error) {
	return nil, nil
}

func (c *fakeDedicatedConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, nil
}

func (c *fakeDedicatedConn) Release() {}

type fakeRow struct{ conn *fakeDedicatedConn }

func (r *fakeRow) Scan(dest ...any) error {
	for _, d := range dest {
		switch v := d.(type) {
		case *bool:
			*v = true
		case *int:
			*v = 1
		}
	}
	r.conn.inUse.Store(false)
	return nil
}

type fakeAcquirer struct{ conn driver.DedicatedConn }

func (f fakeAcquirer) AcquireConn(context.Context) (driver.DedicatedConn, error) {
	return f.conn, nil
}

// TestElector_DedicatedConnNotUsedConcurrentlyOnAbdication reproduces the CI
// data race: when lead() returns, the abdication path runs pg_advisory_unlock
// on the dedicated connection while the liveness watchdog may still be mid
// query on that same connection. campaign must join the watchdog before
// touching the connection again.
func TestElector_DedicatedConnNotUsedConcurrentlyOnAbdication(t *testing.T) {
	watchdogTicked := make(chan struct{}, 1)
	conn := &fakeDedicatedConn{
		hook: func(query string) {
			if query == `SELECT 1` {
				select {
				case watchdogTicked <- struct{}{}:
				default:
				}
				// Hold the connection long enough that, absent proper
				// synchronization, the abdication unlock overlaps with us.
				time.Sleep(50 * time.Millisecond)
			}
		},
	}

	e := &Elector{
		pg:        fakeAcquirer{conn: conn},
		key:       1,
		retry:     time.Hour,
		heartbeat: time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := e.campaign(ctx, func(leadCtx context.Context) error {
		// Abdicate only once the watchdog is demonstrably inside a query.
		<-watchdogTicked
		return nil
	})
	if err != nil {
		t.Fatalf("campaign: %v", err)
	}

	if conn.overlap.Load() {
		t.Fatal("dedicated connection used by two goroutines concurrently during abdication")
	}
}
