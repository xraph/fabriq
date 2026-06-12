package main

import (
	"encoding/base64"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/subscribe"
)

// Document-plane demo endpoints (grove-crdt clients):
//
//	POST /api/v1/docs/update     {"doc_id": "...", "update": <base64>}
//	POST /api/v1/docs/sync       {"doc_id": "...", "state_vector": <base64>}
//	GET  /api/v1/docs/subscribe?id=<doc_id>&token=...   (SSE, RAW frames)
//
// Fetch-then-subscribe for documents: sync first, then attach; a seq gap
// in the live frames means "sync again". Frames are never conflated.

type docUpdateRequest struct {
	DocID  string `json:"doc_id"`
	Update []byte `json:"update"` // JSON []crdt.ChangeRecord (base64 over the wire)
}

func (s *server) docUpdate(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var req docUpdateRequest
	if bindErr := ctx.Bind(&req); bindErr != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": bindErr.Error()})
	}
	if err := s.fabric.Document().ApplyUpdate(tctx, req.DocID, req.Update); err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusAccepted, map[string]string{"status": "applied"})
}

type docSyncRequest struct {
	DocID       string `json:"doc_id"`
	StateVector string `json:"state_vector"` // base64; empty = from scratch
}

func (s *server) docSync(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var req docSyncRequest
	if bindErr := ctx.Bind(&req); bindErr != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": bindErr.Error()})
	}
	vector, _ := base64.StdEncoding.DecodeString(req.StateVector)
	blob, err := s.fabric.Document().Sync(tctx, req.DocID, vector)
	if err != nil {
		return writeCommandError(ctx, err)
	}
	ctx.Response().Header().Set("Content-Type", "application/json")
	_, _ = ctx.Response().Write(blob)
	return nil
}

// docSubscribe streams RAW sync frames over SSE: id = stream id,
// event = <entity>.sync, data = the delta (Payload carries the update
// blob, Version the log seq for gap detection).
func (s *server) docSubscribe(ctx forge.Context) error {
	w, r := ctx.Response(), ctx.Request()
	tctx, err := s.auth.Authenticate(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return nil
	}
	frames, err := s.fabric.SubscribeDocument(tctx, r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return nil
	}
	sse, err := subscribe.NewSSEWriter(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-tctx.Done():
			return nil
		case frame, ok := <-frames:
			if !ok {
				return nil
			}
			if err := sse.WriteDelta(frame); err != nil {
				return nil
			}
		case <-heartbeat.C:
			if err := sse.Heartbeat(); err != nil {
				return nil
			}
		}
	}
}
