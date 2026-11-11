package falkordb

import (
	"context"
	"net"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// PingReadiness checks FalkorDB on its own connection, which closes when the
// caller cancels or the adapter shuts down.
func (a *Adapter) PingReadiness(ctx context.Context) (err error) {
	// Close is the only writer.
	if !a.readinessMu.TryRLock() {
		return redis.ErrClosed
	}
	defer a.readinessMu.RUnlock()
	if a.readinessCtx.Err() != nil {
		return redis.ErrClosed
	}
	defer func() {
		if a.readinessCtx.Err() != nil {
			err = redis.ErrClosed
		} else if err == nil {
			err = ctx.Err()
		}
	}()

	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopOnShutdown := context.AfterFunc(a.readinessCtx, cancel)
	defer stopOnShutdown()

	opts := a.client.Options()
	conn, err := opts.Dialer(probeCtx, opts.Network, opts.Addr)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	stop := context.AfterFunc(probeCtx, func() {
		_ = conn.Close()
		close(done)
	})
	defer func() {
		if !stop() {
			<-done
		}
		_ = conn.Close()
	}()

	probe := redis.NewClient(&redis.Options{
		Addr:                  opts.Addr,
		Network:               opts.Network,
		Username:              opts.Username,
		Password:              opts.Password,
		Protocol:              opts.Protocol,
		Dialer:                func(context.Context, string, string) (net.Conn, error) { return conn, nil },
		MaxRetries:            -1,
		DialerRetries:         1,
		PoolSize:              1,
		ContextTimeoutEnabled: true,
		DisableIdentity:       true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer func() { _ = probe.Close() }()
	return probe.Ping(probeCtx).Err()
}
