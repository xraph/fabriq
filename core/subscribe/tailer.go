package subscribe

import (
	"context"

	"github.com/xraph/fabriq/core/query"
)

// Tailer is the transport port the hub pumps deltas from (the Redis
// adapter implements it over change-channel streams; fakes drive it in
// tests). Tail blocks, delivering every entry after fromID until ctx ends.
// ReadRange is the catch-up read used for Last-Event-ID resume.
type Tailer interface {
	Tail(ctx context.Context, channel, fromID string, deliver func(query.Delta)) error
	ReadRange(ctx context.Context, channel, afterID string, limit int) ([]query.Delta, error)
}

// WithTailer wires a transport pump into the hub: each channel's pump
// starts with its first subscriber (tailing from "now") and stops with its
// last.
func WithTailer(t Tailer) HubOption {
	return func(h *Hub) { h.tailer = t }
}
