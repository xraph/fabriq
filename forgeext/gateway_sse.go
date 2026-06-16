package forgeext

import (
	"encoding/json"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/gateway"
)

// liveSSEController terminates live queries over Server-Sent Events. The client
// POSTs a LiveQuery and receives a uniform event stream: the snapshot folded in
// as reset+enter events, then live enter/leave/move/update events. Reanchor is
// expressed as a reconnect with a new cursor (a fresh snapshot at the new
// anchor); unsubscribe is a disconnect. So this is a single, stateless endpoint.
type liveSSEController struct {
	ext *GatewayExtension
}

func newLiveSSEController(ext *GatewayExtension) *liveSSEController {
	return &liveSSEController{ext: ext}
}

func (c *liveSSEController) Name() string { return "fabriq:live:sse" }

func (c *liveSSEController) Routes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithMethod(http.MethodPost),
		forge.WithName("fabriq.live.sse"),
		forge.WithSummary("Live Query (SSE)"),
		forge.WithTags("Fabriq", "Live", "SSE"),
	}, c.ext.cfg.RouteOptions...)
	return r.SSE(c.ext.cfg.BasePath, c.Stream, opts...)
}

// Stream opens the SSE stream for a posted LiveQuery.
func (c *liveSSEController) Stream(ctx forge.Context) error {
	be, err := c.ext.resolveBackend()
	if err != nil {
		return forge.InternalError(err)
	}

	var q livequery.LiveQuery
	if err := json.NewDecoder(ctx.Request().Body).Decode(&q); err != nil {
		return forge.BadRequest("invalid live query: " + err.Error())
	}

	// The subscription is bound to the request context, so a client disconnect
	// cancels it and tears down the backend subscription.
	sub, err := be.Subscribe(ctx.Request().Context(), q)
	if err != nil {
		return forge.BadRequest("subscribe failed: " + err.Error())
	}
	defer sub.Close()

	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return forge.InternalError(err)
	}
	return gateway.ServeSSE(ctx.Request().Context(), sse, sub, c.ext.cfg.SSE)
}

var _ forge.Controller = (*liveSSEController)(nil)
