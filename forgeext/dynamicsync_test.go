package forgeext

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
)

// logEntry is one captured log call.
type logEntry struct {
	level  string
	msg    string
	fields map[string]any
}

// recordingLogger captures Info and Warn calls and drops the rest.
type recordingLogger struct {
	forge.Logger
	mu      sync.Mutex
	entries []logEntry
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{Logger: forge.NewNoopLogger()}
}

func (l *recordingLogger) record(level, msg string, fields []forge.Field) {
	e := logEntry{level: level, msg: msg, fields: map[string]any{}}
	for _, f := range fields {
		e.fields[f.Key()] = f.Value()
	}
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
}

func (l *recordingLogger) Info(msg string, fields ...forge.Field) { l.record("info", msg, fields) }
func (l *recordingLogger) Warn(msg string, fields ...forge.Field) { l.record("warn", msg, fields) }

func (l *recordingLogger) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logEntry(nil), l.entries...)
}

func TestRunDynamicSyncLoop_TicksUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSyncLoop(ctx, 10*time.Millisecond, func(context.Context) (fabriq.DynamicSyncResult, error) {
			calls.Add(1)
			return fabriq.DynamicSyncResult{}, nil
		}, forge.NewNoopLogger())
	}()
	time.Sleep(55 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not return after cancel")
	}
	if got := calls.Load(); got < 3 {
		t.Fatalf("sync called %d times, want at least 3", got)
	}
}

func TestRunDynamicSyncLoop_ContinuesAfterError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSyncLoop(ctx, 5*time.Millisecond, func(context.Context) (fabriq.DynamicSyncResult, error) {
			if calls.Add(1) == 1 {
				return fabriq.DynamicSyncResult{}, errors.New("pg down")
			}
			return fabriq.DynamicSyncResult{}, nil
		}, log)
	}()
	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("sync called %d times, want the loop to carry on past the error", calls.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
	var warned bool
	for _, e := range log.snapshot() {
		if e.level == "warn" {
			warned = true
		}
	}
	if !warned {
		t.Fatal("a sync error should be logged at warn")
	}
}

func TestRunDynamicSyncLoop_LogsSkipped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runDynamicSyncLoop(ctx, 5*time.Millisecond, func(context.Context) (fabriq.DynamicSyncResult, error) {
			// Report the skip on the first pass only; later passes are quiet.
			if calls.Add(1) == 1 {
				return fabriq.DynamicSyncResult{
					Added:   []string{"good"},
					Skipped: map[string]string{"bad": "invalid spec: x"},
				}, nil
			}
			return fabriq.DynamicSyncResult{}, nil
		}, log)
	}()
	deadline := time.After(2 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("loop never ticked twice")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	var warns, infos int
	for _, e := range log.snapshot() {
		switch e.level {
		case "warn":
			warns++
			if e.fields["entity"] != "bad" || e.fields["reason"] != "invalid spec: x" {
				t.Fatalf("warn fields = %v, want entity=bad reason=invalid spec: x", e.fields)
			}
		case "info":
			infos++
		}
	}
	if warns != 1 {
		t.Fatalf("got %d warn entries, want exactly 1", warns)
	}
	if infos < 1 {
		t.Fatal("an added type should be logged at info")
	}
}
