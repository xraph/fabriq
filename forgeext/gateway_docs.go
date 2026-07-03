package forgeext

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/subscribe"
)

// docFacade is the slice of the fabriq facade the document endpoints
// consume — an interface so handlers are unit-testable without a full
// Open. *fabriq.Fabriq satisfies it.
type docFacade interface {
	Document() document.Store
	SubscribeDocument(ctx context.Context, docID string) (<-chan query.Delta, error)
	SubscribeDocumentPresence(ctx context.Context, docID string) (<-chan query.Delta, error)
	PublishDocumentPresence(ctx context.Context, docID, nodeID string, data json.RawMessage) error
}

// docsController terminates the document plane at the gateway edge:
//
//	POST {BasePath}/docs/update    {"docId","update"(base64)}  → 202
//	POST {BasePath}/docs/sync     {"docId","stateVector"(base64)} → sync payload
//	POST {BasePath}/docs/presence {"docId","node","data"}      → 202
//	SSE  {BasePath}/docs/subscribe?id=<docID>[&presence=1]     → RAW frames
//
// Fetch-then-subscribe: sync first, then attach; a gap in frame seqs means
// "sync again". Frames are never conflated. Tenancy comes from the request
// context — the host's auth middleware (RouteOptions) must stamp it;
// fabriq stays auth-scheme-agnostic.
type docsController struct {
	ext *GatewayExtension
	// facade overrides ext.fab.Fabriq() in tests.
	facade docFacade
}

func newDocsController(ext *GatewayExtension) *docsController {
	return &docsController{ext: ext}
}

func (c *docsController) Name() string { return "fabriq:docs" }

func (c *docsController) resolveFacade() (docFacade, error) {
	if c.facade != nil {
		return c.facade, nil
	}
	f := c.ext.fab.Fabriq()
	if f == nil {
		return nil, forge.InternalError(errNotStarted{})
	}
	return f, nil
}

type errNotStarted struct{}

func (errNotStarted) Error() string { return "fabriq-gateway: fabriq facade not started" }

func (c *docsController) Routes(r forge.Router) error {
	base := c.ext.cfg.BasePath + "/docs"
	post := func(name, summary string) []forge.RouteOption {
		return append([]forge.RouteOption{
			forge.WithMethod(http.MethodPost),
			forge.WithName(name),
			forge.WithSummary(summary),
			forge.WithTags("Fabriq", "Documents", "CRDT"),
		}, c.ext.cfg.RouteOptions...)
	}
	if err := r.POST(base+"/update", c.Update, post("fabriq.docs.update", "Apply CRDT document update")...); err != nil {
		return err
	}
	if err := r.POST(base+"/sync", c.Sync, post("fabriq.docs.sync", "Document state-vector sync")...); err != nil {
		return err
	}
	if err := r.POST(base+"/presence", c.Presence, post("fabriq.docs.presence", "Publish document presence")...); err != nil {
		return err
	}
	sseOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.docs.subscribe"),
		forge.WithSummary("Document sync frames (SSE, RAW)"),
		forge.WithTags("Fabriq", "Documents", "CRDT", "SSE"),
	}, c.ext.cfg.RouteOptions...)
	return r.SSE(base+"/subscribe", c.Subscribe, sseOpts...)
}

type docUpdateRequest struct {
	DocID  string `json:"docId"`
	Update []byte `json:"update"` // JSON []crdt.ChangeRecord, base64 over the wire
}

// Update appends one CRDT update; the live fan-out publishes it to
// collaborators immediately.
func (c *docsController) Update(ctx forge.Context) error {
	f, err := c.resolveFacade()
	if err != nil {
		return err
	}
	var req docUpdateRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid update request: " + err.Error())
	}
	if req.DocID == "" || len(req.Update) == 0 {
		return forge.BadRequest("docId and update are required")
	}
	if err := f.Document().ApplyUpdate(ctx.Request().Context(), req.DocID, req.Update); err != nil {
		return forge.BadRequest("apply failed: " + err.Error())
	}
	return ctx.JSON(http.StatusAccepted, map[string]string{"status": "applied"})
}

type docSyncRequest struct {
	DocID       string `json:"docId"`
	StateVector []byte `json:"stateVector,omitempty"` // 8-byte big-endian seq, base64
}

// Sync returns the updates the client is missing (snapshot + tail) for its
// state vector. The response body is the raw sync payload JSON.
func (c *docsController) Sync(ctx forge.Context) error {
	f, err := c.resolveFacade()
	if err != nil {
		return err
	}
	var req docSyncRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid sync request: " + err.Error())
	}
	if req.DocID == "" {
		return forge.BadRequest("docId is required")
	}
	payload, err := f.Document().Sync(ctx.Request().Context(), req.DocID, req.StateVector)
	if err != nil {
		return forge.BadRequest("sync failed: " + err.Error())
	}
	ctx.Response().Header().Set("Content-Type", "application/json")
	_, werr := ctx.Response().Write(payload)
	return werr
}

type docPresenceRequest struct {
	DocID string          `json:"docId"`
	Node  string          `json:"node"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// Presence publishes one ephemeral awareness frame (heartbeat-style:
// clients re-send on their own cadence; nothing is persisted).
func (c *docsController) Presence(ctx forge.Context) error {
	f, err := c.resolveFacade()
	if err != nil {
		return err
	}
	var req docPresenceRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid presence request: " + err.Error())
	}
	if req.DocID == "" || req.Node == "" {
		return forge.BadRequest("docId and node are required")
	}
	if err := f.PublishDocumentPresence(ctx.Request().Context(), req.DocID, req.Node, req.Data); err != nil {
		return forge.BadRequest("presence failed: " + err.Error())
	}
	return ctx.JSON(http.StatusAccepted, map[string]string{"status": "published"})
}

// Subscribe streams a document's RAW sync frames over SSE (event "sync",
// id = log seq — a gap means "call sync and resume"). With ?presence=1 the
// stream interleaves awareness frames (event "presence").
func (c *docsController) Subscribe(ctx forge.Context) error {
	f, err := c.resolveFacade()
	if err != nil {
		return err
	}
	r := ctx.Request()
	docID := r.URL.Query().Get("id")
	if docID == "" {
		return forge.BadRequest("id query parameter is required")
	}
	reqCtx := r.Context()

	frames, err := f.SubscribeDocument(reqCtx, docID)
	if err != nil {
		return forge.BadRequest("subscribe failed: " + err.Error())
	}
	var presence <-chan query.Delta
	if r.URL.Query().Get("presence") != "" {
		presence, err = f.SubscribeDocumentPresence(reqCtx, docID)
		if err != nil {
			return forge.BadRequest("presence subscribe failed: " + err.Error())
		}
	}

	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return forge.InternalError(err)
	}
	return serveDocSSE(reqCtx, sse, frames, presence, c.ext.cfg.SSE.HeartbeatInterval)
}

// serveDocSSE pumps sync + presence frames until the client disconnects or
// a source closes, with periodic heartbeats.
func serveDocSSE(ctx context.Context, sink *subscribe.SSEWriter, frames, presence <-chan query.Delta, heartbeat time.Duration) error {
	if heartbeat <= 0 {
		heartbeat = 25 * time.Second
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-frames:
			if !ok {
				return nil
			}
			if err := sink.WriteEvent(d.StreamID, "sync", d); err != nil {
				return err
			}
		case d, ok := <-presence:
			if !ok {
				presence = nil // sync frames keep flowing
				continue
			}
			if err := sink.WriteEvent(d.StreamID, "presence", d); err != nil {
				return err
			}
		case <-ticker.C:
			if err := sink.Heartbeat(); err != nil {
				return err
			}
		}
	}
}

var _ forge.Controller = (*docsController)(nil)
