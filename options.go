package fabriq

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/crypto"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/livequery"
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
	liveAuthz        livequery.AuthzFunc
	liveCushion      int
	encryptor        crypto.Encryptor
}

func defaultSettings() settings {
	return settings{
		conflationWindow: 150 * time.Millisecond,
		subscribeBuffer:  64,
		waitPollInterval: 25 * time.Millisecond,
		streamMaxLen:     500,
		liveCushion:      16,
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

// WithLiveAuthz installs the authorization hook run before a live query's
// snapshot. It may also be used (in later phases) to inject mandatory
// row-visibility predicates into the query's filter.
func WithLiveAuthz(fn livequery.AuthzFunc) Option {
	return func(s *settings) { s.liveAuthz = fn }
}

// WithLiveCushion sets how many extra rows beyond the visible window each
// maintained live query buffers, to absorb boundary churn before a Postgres
// refill is needed (default 16).
func WithLiveCushion(n int) Option {
	return func(s *settings) {
		if n > 0 {
			s.liveCushion = n
		}
	}
}

// WithLifecycleHook appends data-lifecycle hooks to the command plane. Each
// hook runs INSIDE the write transaction after every change is staged: it may
// write its own rows atomically (via the tx handle) or veto the write by
// returning an error (which rolls the whole command back). Hooks fire in
// registration order across all WithLifecycleHook calls. This is the in-tx,
// cross-cutting seam an auditing/chronicle extension hooks into.
func WithLifecycleHook(hooks ...command.LifecycleHook) Option {
	return func(s *settings) {
		s.executorOptions = append(s.executorOptions, command.WithHooks(hooks...))
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

// WithEncryptor sets the field encryptor used for blob_source credentials.
func WithEncryptor(e crypto.Encryptor) Option {
	return func(s *settings) { s.encryptor = e }
}
