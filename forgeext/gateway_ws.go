package forgeext

import (
	"github.com/xraph/forge"

	"github.com/xraph/fabriq/gateway"
)

// liveWSController terminates live queries over WebSocket. Forge owns the
// upgrade and hands the handler a forge.Connection (which satisfies
// gateway.WSConn), so there is no third-party WebSocket dependency. The socket
// is bidirectional: deltas stream down as frames while the client sends
// subscribe/reanchor/unsubscribe commands up.
type liveWSController struct {
	ext *GatewayExtension
}

func newLiveWSController(ext *GatewayExtension) *liveWSController {
	return &liveWSController{ext: ext}
}

func (c *liveWSController) Name() string { return "fabriq:live:ws" }

func (c *liveWSController) Routes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.live.ws"),
		forge.WithSummary("Live Query (WebSocket)"),
		forge.WithTags("Fabriq", "Live", "WebSocket"),
	}, c.ext.cfg.RouteOptions...)
	return r.WebSocket(c.ext.cfg.BasePath+"/ws", c.Connect, opts...)
}

// Connect runs one WebSocket subscription lifecycle.
func (c *liveWSController) Connect(_ forge.Context, conn forge.Connection) error {
	be, err := c.ext.resolveBackend()
	if err != nil {
		return err
	}
	return gateway.ServeWS(conn.Context(), conn, be, c.ext.cfg.WS)
}

var _ forge.Controller = (*liveWSController)(nil)
