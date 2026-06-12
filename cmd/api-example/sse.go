package main

import (
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/subscribe"
)

// subscribe is the fetch-then-subscribe SSE endpoint:
//
//	GET /api/v1/subscribe?entity=asset&scope=site&id=S1&token=...
//
// The client first fetches current state over REST, then attaches here.
// The channel is resolved server-side from the validated scope — the
// client never names a channel or tenant. Reconnects send Last-Event-ID
// and receive the missed deltas (short channels; a full page means
// "refetch instead").
//
// Proxy-safety lives in subscribe.SSEWriter: explicit flush after every
// event, X-Accel-Buffering: no, heartbeats every 15s.
func (s *server) subscribe(ctx forge.Context) error {
	s.subscribeHTTP(ctx.Response(), ctx.Request())
	return nil
}

// subscribeHTTP is the stdlib-shaped SSE handler (kept separate so the
// integration test can serve it over a real streaming connection).
func (s *server) subscribeHTTP(w http.ResponseWriter, r *http.Request) {
	tctx, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	scope := query.SubscribeScope{
		Entity: r.URL.Query().Get("entity"),
		Scope:  r.URL.Query().Get("scope"),
		ID:     r.URL.Query().Get("id"),
	}

	// Attach to live deltas BEFORE catching up, so nothing falls in the
	// gap (overlap is fine: clients dedupe by SSE id). tctx derives from
	// r.Context(), so client disconnect tears the subscription down.
	deltas, err := s.fabric.Subscribe(tctx, scope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	sse, err := subscribe.NewSSEWriter(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if lastID := subscribe.LastEventID(r); lastID != "" {
		missed, err := s.fabric.CatchUp(tctx, scope, lastID, 256)
		if err != nil {
			_ = sse.WriteEvent("", "error", map[string]string{"error": err.Error()})
			return
		}
		for _, d := range missed {
			if err := sse.WriteDelta(d); err != nil {
				return // client gone
			}
		}
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-tctx.Done():
			return
		case d, ok := <-deltas:
			if !ok {
				return
			}
			if err := sse.WriteDelta(d); err != nil {
				return // client gone
			}
		case <-heartbeat.C:
			if err := sse.Heartbeat(); err != nil {
				return
			}
		}
	}
}
