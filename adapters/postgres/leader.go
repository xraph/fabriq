package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/grove/driver"
)

// Elector provides advisory-lock leadership for singleton runners (the
// outbox relay, the reconciler). It holds pg_try_advisory_lock on a
// DEDICATED pooled connection (grove's ConnAcquirer), so the session-level
// lock cannot leak to other pool users; a connection-liveness watchdog
// abdicates if the session dies (the lock died with it).
//
// fabriq-worker can therefore run any number of replicas: exactly one
// holds each role.
type Elector struct {
	pg interface {
		AcquireConn(ctx context.Context) (driver.DedicatedConn, error)
	}
	key       int64
	retry     time.Duration
	heartbeat time.Duration
}

// ElectorOption tunes the elector.
type ElectorOption func(*Elector)

// WithElectorRetry sets how often a non-leader retries acquisition
// (default 5s).
func WithElectorRetry(d time.Duration) ElectorOption {
	return func(e *Elector) {
		if d > 0 {
			e.retry = d
		}
	}
}

// WithElectorHeartbeat sets the leader's session-liveness check cadence
// (default 5s).
func WithElectorHeartbeat(d time.Duration) ElectorOption {
	return func(e *Elector) {
		if d > 0 {
			e.heartbeat = d
		}
	}
}

// NewElector builds an elector for an advisory lock key. Pick one key per
// role (e.g. relay, reconciler) and keep them stable across versions.
func NewElector(a *Adapter, key int64, opts ...ElectorOption) *Elector {
	e := &Elector{pg: a.pg, key: key, retry: 5 * time.Second, heartbeat: 5 * time.Second}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Run keeps trying to lead until ctx ends. While leading it runs lead with
// a context that is cancelled on abdication (session loss or ctx end).
// lead returning (even nil) abdicates and re-campaigns.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := e.campaign(ctx, lead); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.retry):
		}
	}
}

// TryLead makes one non-blocking claim: if the advisory lock is free it
// runs lead while holding it and reports true; if another session holds
// the lock it reports false immediately without running lead. This is the
// catalog-mode sweeper's per-tenant-database work claim — many sweeper
// replicas race, exactly one does each tenant's pass.
func (e *Elector) TryLead(ctx context.Context, lead func(ctx context.Context) error) (bool, error) {
	return e.campaign(ctx, lead)
}

// campaign makes one claim attempt, reporting whether the lock was won.
func (e *Elector) campaign(ctx context.Context, lead func(ctx context.Context) error) (bool, error) {
	conn, err := e.pg.AcquireConn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, e.key).Scan(&got); err != nil {
		return false, fmt.Errorf("fabriq: try advisory lock: %w", err)
	}
	if !got {
		return false, nil
	}

	leadCtx, cancel := context.WithCancel(ctx)

	// Watchdog: the advisory lock lives in the dedicated session. If that
	// session dies, someone else may already lead — abdicate immediately.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(e.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-leadCtx.Done():
				return
			case <-ticker.C:
				var one int
				if err := conn.QueryRow(leadCtx, `SELECT 1`).Scan(&one); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// Tear down in order: stop the watchdog and wait for it to exit BEFORE
	// reusing the dedicated connection for the unlock — a pgx connection is
	// not safe for concurrent use, and the watchdog and unlock both query it.
	defer func() {
		cancel()
		wg.Wait()
		// Best effort: if the session is gone the lock is already free.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer releaseCancel()
		var released bool
		_ = conn.QueryRow(releaseCtx, `SELECT pg_advisory_unlock($1)`, e.key).Scan(&released)
	}()

	return true, lead(leadCtx)
}
