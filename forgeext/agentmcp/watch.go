package agentmcp

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/subscribe"
)

const defaultWatchHeartbeat = 15 * time.Second

// serveDeltaSSE streams query.Deltas from ch as SSE events until ctx is done
// or the channel closes, with periodic heartbeats.
func serveDeltaSSE(ctx context.Context, sink *subscribe.SSEWriter, ch <-chan query.Delta, heartbeat time.Duration) error {
	if heartbeat <= 0 {
		heartbeat = defaultWatchHeartbeat
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-ch:
			if !ok {
				return nil
			}
			if err := sink.WriteEvent(d.StreamID, d.Type, d); err != nil {
				return err
			}
		case <-ticker.C:
			if err := sink.Heartbeat(); err != nil {
				return err
			}
		}
	}
}

type watchController struct{ ext *Extension }

func newWatchController(e *Extension) *watchController { return &watchController{ext: e} }

func (c *watchController) Name() string { return "fabriq:agent:mcp:watch" }

func (c *watchController) Routes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithMethod(http.MethodPost),
		forge.WithName("fabriq.agent.mcp.watch"),
		forge.WithSummary("Agent toolkit watch (SSE delta stream)"),
		forge.WithTags("Fabriq", "Agent", "MCP", "SSE"),
	}, c.ext.cfg.RouteOptions...)
	return r.SSE(c.ext.cfg.WatchPath, c.Stream, opts...)
}

// Stream opens the SSE stream for a posted SubscribeScope.
func (c *watchController) Stream(ctx forge.Context) error {
	tk, err := c.ext.resolveToolkit()
	if err != nil {
		return forge.InternalError(err)
	}
	var scope query.SubscribeScope
	if err = json.NewDecoder(ctx.Request().Body).Decode(&scope); err != nil {
		return forge.BadRequest("invalid subscribe scope: " + err.Error())
	}
	ch, err := tk.Watch(ctx.Request().Context(), scope)
	if err != nil {
		return forge.BadRequest("watch failed: " + err.Error())
	}
	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return forge.InternalError(err)
	}
	return serveDeltaSSE(ctx.Request().Context(), sse, ch, defaultWatchHeartbeat)
}

var _ forge.Controller = (*watchController)(nil)
