package subscribe

import (
	"context"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// AuthzFunc authorizes a subscription request before any channel is
// resolved. Implementations get the validated scope and the (already
// tenant-stamped) context; returning an error denies the subscription.
type AuthzFunc func(ctx context.Context, req query.SubscribeScope) error

// AllowAll is the default authorization hook.
func AllowAll(context.Context, query.SubscribeScope) error { return nil }

// Gate combines the authz hook with server-side channel resolution; the
// facade's Subscribe goes through a Gate.
type Gate struct {
	reg   *registry.Registry
	authz AuthzFunc
}

// NewGate builds a Gate; a nil authz defaults to AllowAll.
func NewGate(reg *registry.Registry, authz AuthzFunc) *Gate {
	if authz == nil {
		authz = AllowAll
	}
	return &Gate{reg: reg, authz: authz}
}

// Resolve authorizes the request and then resolves its channel.
func (g *Gate) Resolve(ctx context.Context, req query.SubscribeScope) (string, error) {
	if err := g.authz(ctx, req); err != nil {
		return "", err
	}
	return ResolveChannel(ctx, g.reg, req)
}
