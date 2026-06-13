package fabriq

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/internal/otel"
)

// settings collects everything Options tune.
type settings struct {
	conflationWindow time.Duration
	subscribeBuffer  int
	waitPollInterval time.Duration
	streamMaxLen     int64
	authz            subscribe.AuthzFunc
	docAuthz         func(ctx context.Context, docID string) error
	upcasters        *event.UpcasterChain
	tailer           subscribe.Tailer
	executorOptions  []command.ExecutorOption
}

func defaultSettings() settings {
	return settings{
		conflationWindow: 150 * time.Millisecond,
		subscribeBuffer:  64,
		waitPollInterval: 25 * time.Millisecond,
		streamMaxLen:     500,
		// One trace spans command -> outbox -> relay -> projection apply:
		// the executor stamps the active W3C traceparent by default.
		executorOptions: []command.ExecutorOption{
			command.WithTraceparent(otel.TraceparentFromContext),
		},
	}
}

// Option customizes a Fabriq.
type Option func(*settings)

// WithConflationWindow tunes the hub's LWW flush window (spec range
// 100–250ms; default 150ms).
func WithConflationWindow(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.conflationWindow = d
		}
	}
}

// WithSubscribeBuffer sets the per-subscriber delta buffer; full buffers
// drop (clients refetch + resume by Last-Event-ID).
func WithSubscribeBuffer(n int) Option {
	return func(s *settings) {
		if n > 0 {
			s.subscribeBuffer = n
		}
	}
}

// WithWaitPollInterval tunes WaitForProjection's poll cadence.
func WithWaitPollInterval(d time.Duration) Option {
	return func(s *settings) {
		if d > 0 {
			s.waitPollInterval = d
		}
	}
}

// WithStreamMaxLen sets the approximate MAXLEN for per-channel Redis
// streams (catch-up depth before clients must refetch; default 500).
func WithStreamMaxLen(n int64) Option {
	return func(s *settings) {
		if n > 0 {
			s.streamMaxLen = n
		}
	}
}

// WithAuthz installs the subscribe-time authorization hook.
func WithAuthz(fn subscribe.AuthzFunc) Option {
	return func(s *settings) { s.authz = fn }
}

// WithDocumentAuthz installs the document-plane authorization hook,
// consulted for BOTH ApplyUpdate (writes) and SubscribeDocument (reads).
// Without it, any authenticated member of the tenant may touch any of
// the tenant's documents.
func WithDocumentAuthz(fn func(ctx context.Context, docID string) error) Option {
	return func(s *settings) { s.docAuthz = fn }
}

// WithUpcasters registers the event payload upcaster chain. Projection
// engines apply it at decode, so appliers only ever see the latest
// payload shape; register one vN->vN+1 step per evolved event type.
func WithUpcasters(chain *event.UpcasterChain) Option {
	return func(s *settings) { s.upcasters = chain }
}

// WithTraceparent supplies the W3C traceparent extractor stamped into
// event envelopes (internal/otel provides the production one).
func WithTraceparent(fn func(context.Context) string) Option {
	return func(s *settings) {
		s.executorOptions = append(s.executorOptions, command.WithTraceparent(fn))
	}
}

// WithClock overrides the command-plane clock (tests).
func WithClock(now func() time.Time) Option {
	return func(s *settings) {
		s.executorOptions = append(s.executorOptions, command.WithClock(now))
	}
}
