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
	closed   bool
}

type channelState struct {
	subs    map[int]chan query.Delta
	nextSub int
	conf    *conflator
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

// Subscribe attaches a buffered subscriber to a channel. The returned
// cancel function detaches and closes the subscriber channel; cancelling
// the context does the same.
func (h *Hub) Subscribe(ctx context.Context, channel string, buffer int) (<-chan query.Delta, func(), error) {
	if buffer <= 0 {
		buffer = 64
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, fmt.Errorf("fabriq: hub is closed")
	}
	cs, ok := h.channels[channel]
	if !ok {
		cs = &channelState{subs: make(map[int]chan query.Delta), conf: newConflator(h.window)}
		h.channels[channel] = cs
	}
	id := cs.nextSub
	cs.nextSub++
	ch := make(chan query.Delta, buffer)
	cs.subs[id] = ch

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if cur, ok := h.channels[channel]; ok {
				if sub, live := cur.subs[id]; live {
					delete(cur.subs, id)
					close(sub)
				}
				if len(cur.subs) == 0 && cur.conf.depth() == 0 {
					delete(h.channels, channel)
				}
			}
		})
	}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				cancel()
			}
		}()
	}
	return ch, cancel, nil
}

// Publish offers a delta to a channel's conflation buffer; survivors are
// fanned out when the window elapses.
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
	if cs.conf.offer(d) {
		cs.conf.timer = time.AfterFunc(h.window, func() { h.flush(channel) })
	}
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

// deliverLocked sends deltas to every subscriber without blocking: full
// buffers drop (clients refetch + resume via Last-Event-ID).
func deliverLocked(cs *channelState, deltas []query.Delta) {
	for _, d := range deltas {
		for _, sub := range cs.subs {
			select {
			case sub <- d:
			default:
			}
		}
	}
}
