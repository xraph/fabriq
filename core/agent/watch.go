// core/agent/watch.go
package agent

import (
	"context"

	"github.com/xraph/fabriq/core/query"
)

// Watch subscribes to the conflated delta stream for a scope, so an agent can
// react to changes as they happen. It is a thin pass-through to the fabric's
// Subscribe (tenant-scoped from ctx). MCP streaming of watch is Phase 4; this
// is the in-process Go path.
func (t *Toolkit) Watch(ctx context.Context, scope query.SubscribeScope) (<-chan query.Delta, error) {
	return t.fab.Subscribe(ctx, scope)
}
