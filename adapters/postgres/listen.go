package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// notifyLoop is the relay's LISTEN/NOTIFY wake-up. It is the ONE place
// fabriq uses pgx directly: receiving asynchronous notifications needs a
// connection parked in WaitForNotification, which grove's driver cannot
// express today (its Listener has a connection-ordering race and no
// upstream usage — see docs/decisions/0005-relay-listen-notify.md).
//
// The loop is purely an optimization: it nudges the wake channel; the
// relay's interval poll remains the correctness mechanism. Connection
// failures back off and reconnect forever.
func notifyLoop(ctx context.Context, dsn, channel string, wake chan<- struct{}) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := listenOnce(ctx, dsn, channel, wake); err != nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func listenOnce(ctx context.Context, dsn, channel string, wake chan<- struct{}) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
