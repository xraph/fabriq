package subscribe

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/xraph/fabriq/core/query"
)

// SSEWriter bridges delta streams onto Server-Sent Events. It is
// deliberately stdlib-only and proxy-safe: it requires an http.Flusher and
// flushes after every event, sets X-Accel-Buffering: no, and maps the
// transport stream ID onto the SSE "id:" field so reconnecting clients
// resume via Last-Event-ID.
type SSEWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

// NewSSEWriter prepares w for event streaming. It fails if the
// ResponseWriter cannot flush — buffering proxies would otherwise hold
// events indefinitely.
func NewSSEWriter(w http.ResponseWriter) (*SSEWriter, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("fabriq: response writer does not support flushing; SSE requires explicit flush")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return &SSEWriter{w: w, f: f}, nil
}

// WriteDelta emits one delta as an SSE event: id = StreamID,
// event = delta type, data = the JSON delta.
func (s *SSEWriter) WriteDelta(d query.Delta) error {
	return s.WriteEvent(d.StreamID, d.Type, d)
}

// WriteEvent emits an arbitrary SSE event and flushes.
func (s *SSEWriter) WriteEvent(id, eventName string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("fabriq: sse marshal: %w", err)
	}
	if id != "" {
		if _, err := fmt.Fprintf(s.w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if eventName != "" {
		if _, err := fmt.Fprintf(s.w, "event: %s\n", eventName); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", raw); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

// Heartbeat writes an SSE comment to keep intermediaries from idling the
// connection out.
func (s *SSEWriter) Heartbeat() error {
	if _, err := fmt.Fprint(s.w, ": ping\n\n"); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

// LastEventID extracts the SSE resume position from a request.
func LastEventID(r *http.Request) string {
	return r.Header.Get("Last-Event-ID")
}
