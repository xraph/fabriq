package postgres

import (
	"context"
	"time"

	"github.com/xraph/grove/drivers/pgdriver"
)

// notifyLoop is the relay's LISTEN/NOTIFY wake-up, built on grove's
// pgdriver.Listener: a dedicated pool connection parked in
// WaitForNotification, with LISTEN/UNLISTEN queued and executed by the
// listen goroutine between waits (see
// docs/decisions/0005-relay-listen-notify.md).
//
// The loop is purely an optimization: it nudges the wake channel; the
// relay's interval poll remains the correctness mechanism. grove's
// Listener does not reconnect — its goroutine exits on fatal connection
// errors — so this wrapper rebuilds it with backoff forever.
func notifyLoop(ctx context.Context, pg *pgdriver.PgDB, channel string, wake chan<- struct{}) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := listenOnce(ctx, pg, channel, wake); err != nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// probeInterval paces the liveness re-LISTEN in listenOnce. grove's
// Listener exposes no death signal, but exec on a stopped listener fails
// fast and LISTEN is idempotent per session, so re-issuing it doubles as
// a health check. A dead listener degrades wake latency to this interval
// at worst until the poll ticker catches it anyway.
const probeInterval = 15 * time.Second

func listenOnce(ctx context.Context, pg *pgdriver.PgDB, channel string, wake chan<- struct{}) error {
	l, err := pg.Listen(ctx, channel, func(*pgdriver.Notification) {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()

	probe := time.NewTicker(probeInterval)
	defer probe.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-probe.C:
			if err := l.Listen(ctx, channel); err != nil {
				return err
			}
		}
	}
}
