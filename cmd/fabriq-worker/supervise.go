package main

import (
	"context"
	"time"

	"github.com/xraph/forge"
)

// supervise keeps a runner alive for the worker's lifetime: every exit is
// LOGGED (never swallowed) and restarted with exponential backoff
// (1s -> 30s cap), resetting after a healthy stretch. Silence was the
// worst failure mode — a dead consumer previously showed up only as lag.
func supervise(ctx context.Context, log forge.Logger, name string, run func(ctx context.Context) error) {
	const (
		baseBackoff  = time.Second
		maxBackoff   = 30 * time.Second
		healthyReset = 5 * time.Minute
	)
	backoff := baseBackoff
	for {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return // orderly shutdown
		}
		if time.Since(started) >= healthyReset {
			backoff = baseBackoff
		}
		if log != nil {
			log.Error("fabriq-worker: runner exited; restarting",
				forge.String("runner", name),
				forge.Duration("backoff", backoff),
				forge.Error(err),
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
