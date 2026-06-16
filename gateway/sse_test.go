package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/livequery"
)

// recordedEvent is one SSE event the fake sink saw.
type recordedEvent struct {
	id    string
	event string
}

type fakeSink struct {
	mu         sync.Mutex
	events     []recordedEvent
	heartbeats int
	failAfter  int // -1 = never; otherwise fail the Nth WriteEvent (0-based)
	deadlines  int
}

func newFakeSink() *fakeSink { return &fakeSink{failAfter: -1} }

func (f *fakeSink) WriteEvent(id, event string, _ any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAfter >= 0 && len(f.events) == f.failAfter {
		return errors.New("client gone")
	}
	f.events = append(f.events, recordedEvent{id, event})
	return nil
}

func (f *fakeSink) SetWriteDeadline(time.Time) error {
	f.mu.Lock()
	f.deadlines++
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) Heartbeat() error {
	f.mu.Lock()
	f.heartbeats++
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.events))
	for i, e := range f.events {
		out[i] = e.event
	}
	return out
}

func subFrom(ch <-chan livequery.LiveDelta) *Sub {
	return NewSub("s1", ch, nil, func() {})
}

func TestServeSSE_ForwardsSnapshotThenLiveDelta(t *testing.T) {
	ch := make(chan livequery.LiveDelta, 8)
	// Snapshot folded into the stream: reset + two enters.
	ch <- livequery.LiveDelta{Op: livequery.OpReset}
	ch <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a", NewIndex: 0, StreamID: "e1"}
	ch <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "b", NewIndex: 1, StreamID: "e2"}
	// Then a live enter.
	ch <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "c", NewIndex: 2, StreamID: "e3"}

	sink := newFakeSink()
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- ServeSSE(ctx, sink, subFrom(ch), SSEOptions{WriteTimeout: time.Second}) }()

	waitFor(t, func() bool { return len(sink.names()) == 4 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ServeSSE returned %v, want nil on ctx cancel", err)
	}

	got := sink.names()
	want := []string{"reset", "enter", "enter", "enter"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if sink.deadlines < 4 {
		t.Fatalf("expected a write deadline set per event, got %d", sink.deadlines)
	}
}

func TestServeSSE_ReturnsNilWhenStreamCloses(t *testing.T) {
	ch := make(chan livequery.LiveDelta)
	close(ch)
	err := ServeSSE(context.Background(), newFakeSink(), subFrom(ch), SSEOptions{})
	if err != nil {
		t.Fatalf("ServeSSE = %v, want nil on closed stream", err)
	}
}

func TestServeSSE_WriteErrorTearsDown(t *testing.T) {
	ch := make(chan livequery.LiveDelta, 4)
	ch <- livequery.LiveDelta{Op: livequery.OpReset}
	ch <- livequery.LiveDelta{Op: livequery.OpEnter, AggID: "a"}
	sink := newFakeSink()
	sink.failAfter = 1 // first write ok, second fails
	err := ServeSSE(context.Background(), sink, subFrom(ch), SSEOptions{})
	if err == nil {
		t.Fatal("ServeSSE = nil, want the write error (teardown)")
	}
}

func TestServeSSE_Heartbeat(t *testing.T) {
	ch := make(chan livequery.LiveDelta) // never sends — only heartbeats
	sink := newFakeSink()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeSSE(ctx, sink, subFrom(ch), SSEOptions{HeartbeatInterval: 5 * time.Millisecond}) }()
	waitFor(t, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.heartbeats >= 2
	})
	cancel()
	<-done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
