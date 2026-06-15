package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/subscribe"
)

// live is the maintained-result-set live query endpoint:
//
//	POST /api/v1/live   (body: a JSON LiveQuery; token in the usual auth header)
//
// It returns an SSE stream: first a `snapshot` event carrying the initial
// ordered window, then `enter`/`leave`/`move`/`update` events as the result
// set changes. The query body (filter + sort + limit) does not fit a query
// string, so the subscribe is a POST that then upgrades to the stream.
func (s *server) live(ctx forge.Context) error {
	s.liveHTTP(ctx.Response(), ctx.Request())
	return nil
}

// liveHTTP is the stdlib-shaped handler (the SSE bridge needs the Flusher).
func (s *server) liveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	tctx, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var q livequery.LiveQuery
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// tctx derives from r.Context(), so client disconnect cancels the
	// subscription and the engine goroutine exits.
	snap, deltas, cancel, err := s.fabric.LiveQuery(tctx, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	defer cancel()

	sse, err := subscribe.NewSSEWriter(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := sse.WriteEvent("", "snapshot", snap); err != nil {
		return // client gone
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
			if err := sse.WriteEvent(d.StreamID, d.Op.String(), d); err != nil {
				return // client gone
			}
		case <-heartbeat.C:
			if err := sse.Heartbeat(); err != nil {
				return
			}
		}
	}
}
