package subscribe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/query"
)

// Hub fans deltas out to subscribers with per-channel last-write-wins
// conflation. It is transport-agnostic: in production a pump goroutine
// reads Redis change-channel streams and calls Publish; unit tests publish
// directly; the future CRDT sync sub-protocol shares the connection layer
// through PublishRaw, which bypasses conflation entirely.
//
// Delivery policy: subscriber channels are buffered; when a buffer is full
// the delta is dropped for that subscriber. The fetch-then-subscribe
// contract makes this safe — clients that fall behind refetch and resume
// from Last-Event-ID.
type Hub struct {
	mu       sync.Mutex
	window   time.Duration
	channels map[string]*channelState
	tailer   Tailer
	closed   bool
}

type channelState struct {
	subs       map[int]chan query.Delta
	nextSub    int
	conf       *conflator
	pumpCancel context.CancelFunc
	// raw channels bypass conflation entirely: every frame, in order —
	// the document-sync contract. Mode is fixed by the first subscriber.
	raw bool
}

// HubOption configures a Hub.
type HubOption func(*Hub)

// WithConflationWindow sets the flush window for delta conflation
// (default 150ms; the spec'd range is 100–250ms).
func WithConflationWindow(d time.Duration) HubOption {
	return func(h *Hub) {
		if d > 0 {
			h.window = d
		}
	}
}

// NewHub builds a Hub.
func NewHub(opts ...HubOption) *Hub {
	h := &Hub{
		window:   150 * time.Millisecond,
		channels: make(map[string]*channelState),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Subscribe attaches a buffered subscriber to a conflated delta channel.
// The returned cancel function detaches and closes the subscriber
// channel; cancelling the context does the same.
func (h *Hub) Subscribe(ctx context.Context, channel string, buffer int) (deltas <-chan query.Delta, cancel func(), err error) {
	return h.subscribe(ctx, channel, buffer, false)
}

// SubscribeRaw attaches to a RAW channel: every frame is delivered
// immediately and in order, with no conflation and no coalescing — the
// document-sync sub-protocol's contract. A channel's mode is fixed by its
// first subscriber; mixing modes on one channel is an error.
func (h *Hub) SubscribeRaw(ctx context.Context, channel string, buffer int) (frames <-chan query.Delta, cancel func(), err error) {
	return h.subscribe(ctx, channel, buffer, true)
}

func (h *Hub) subscribe(ctx context.Context, channel string, buffer int, raw bool) (deltas <-chan query.Delta, cancel func(), err error) {
	if buffer <= 0 {
		buffer = 64
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, fmt.Errorf("fabriq: hub is closed")
	}
	cs, ok := h.channels[channel]
	if ok && cs.raw != raw {
		return nil, nil, fmt.Errorf("fabriq: channel %q is %s; one delivery mode per channel",
			channel, modeName(cs.raw))
	}
	if !ok {
		cs = &channelState{subs: make(map[int]chan query.Delta), conf: newConflator(h.window), raw: raw}
		h.channels[channel] = cs
		if h.tailer != nil {
			pumpCtx, pumpCancel := context.WithCancel(context.Background())
			cs.pumpCancel = pumpCancel
			go func() {
				// Errors end the pump; subscribers fall back to the
				// refetch contract on silence.
				_ = h.tailer.Tail(pumpCtx, channel, "$", func(d query.Delta) {
					h.Publish(channel, d)
				})
			}()
		}
	}
	id := cs.nextSub
	cs.nextSub++
	ch := make(chan query.Delta, buffer)
	cs.subs[id] = ch

	var once sync.Once
	cancel = func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if cur, ok := h.channels[channel]; ok {
				if sub, live := cur.subs[id]; live {
					delete(cur.subs, id)
					close(sub)
				}
				if len(cur.subs) == 0 {
					if cur.pumpCancel != nil {
						cur.pumpCancel()
						cur.pumpCancel = nil
					}
					if cur.conf.depth() == 0 {
						delete(h.channels, channel)
					}
				}
			}
		})
	}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}
	return ch, cancel, nil
}

// Publish offers a delta to a channel. Conflated channels buffer it (LWW
// per aggregate, window flush); raw channels deliver immediately in
// order. The pump calls this for every transport entry, so the channel's
// mode decides the semantics.
func (h *Hub) Publish(channel string, d query.Delta) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	cs, ok := h.channels[channel]
	if !ok {
		return // nobody listening; deltas live on in Redis for catch-up
	}
	if cs.raw {
		deliverLocked(cs, []query.Delta{d})
		return
	}
	if cs.conf.offer(d) {
		cs.conf.timer = time.AfterFunc(h.window, func() { h.flush(channel) })
	}
}

func modeName(raw bool) string {
	if raw {
		return "raw"
	}
	return "conflated"
}

// PublishRaw bypasses conflation: ordered, complete delivery for protocols
// that cannot tolerate coalescing (CRDT sync).
func (h *Hub) PublishRaw(channel string, d query.Delta) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	if cs, ok := h.channels[channel]; ok {
		deliverLocked(cs, []query.Delta{d})
	}
}

// Flush forces all pending conflation buffers out immediately (shutdown
// drain and deterministic tests).
func (h *Hub) Flush() {
	h.mu.Lock()
	channels := make([]string, 0, len(h.channels))
	for name := range h.channels {
		channels = append(channels, name)
	}
	h.mu.Unlock()
	for _, name := range channels {
		h.flush(name)
	}
}

// ConflationDepth reports the total buffered (not yet flushed) delta count
// — exported as a gauge by internal/metrics.
func (h *Hub) ConflationDepth() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	total := 0
	for _, cs := range h.channels {
		total += cs.conf.depth()
	}
	return total
}

// Close stops the hub and closes every subscriber channel.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for name, cs := range h.channels {
		if cs.conf.timer != nil {
			cs.conf.timer.Stop()
		}
		if cs.pumpCancel != nil {
			cs.pumpCancel()
		}
		for id, sub := range cs.subs {
			delete(cs.subs, id)
			close(sub)
		}
		delete(h.channels, name)
	}
}

func (h *Hub) flush(channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cs, ok := h.channels[channel]
	if !ok {
		return
	}
	if cs.conf.timer != nil {
		cs.conf.timer.Stop()
		cs.conf.timer = nil
	}
	deliverLocked(cs, cs.conf.drain())
	if len(cs.subs) == 0 && cs.conf.depth() == 0 {
		delete(h.channels, channel)
	}
}

// deliverLocked sends deltas to every subscriber without blocking.
// Conflated channels drop on a full buffer (clients refetch + resume via
// Last-Event-ID). Raw channels must deliver complete-and-in-order or
// nothing: an overflowing raw subscriber is CLOSED so its client knows to
// re-Sync instead of silently missing a frame.
func deliverLocked(cs *channelState, deltas []query.Delta) {
	for _, d := range deltas {
		for id, sub := range cs.subs {
			select {
			case sub <- d:
			default:
				if cs.raw {
					delete(cs.subs, id)
					close(sub)
				}
			}
		}
	}
}
