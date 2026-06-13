package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSupervise_RestartsFailingRunnerAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var runs atomic.Int32

	done := make(chan struct{})
	go func() {
		defer close(done)
		supervise(ctx, nil, "flaky", func(runCtx context.Context) error {
			if runs.Add(1) >= 3 {
				<-runCtx.Done() // healthy on the third try
				return runCtx.Err()
			}
			return errors.New("transient crash")
		})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for runs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if runs.Load() < 3 {
		t.Fatalf("runner restarted %d times, want >= 3", runs.Load())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervise did not stop on ctx cancel")
	}
}
