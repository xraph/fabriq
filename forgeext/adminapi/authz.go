package adminapi

import (
	"context"
	"errors"
	"log/slog"

	"github.com/xraph/forge"
)

// Authorizer decides whether the current request may exercise a named capability.
// ctx is the request context — it carries the resolved tenant and API-key id, plus
// any principal a host authz middleware (e.g. warden) stamped before the handler ran.
// A resource-free (ctx, capability) shape is the minimal common denominator: the
// tenant is already in ctx, so a per-tenant authz system reads it from there.
type Authorizer interface {
	Authorize(ctx context.Context, capability string) (allowed bool, err error)
}

// AuthorizerFunc adapts a plain function to Authorizer.
type AuthorizerFunc func(ctx context.Context, capability string) (bool, error)

// Authorize implements Authorizer.
func (f AuthorizerFunc) Authorize(ctx context.Context, capability string) (bool, error) {
	return f(ctx, capability)
}

// flagAuthorizer is the default authorizer: it reproduces the historical
// global-flag gate decisions from the host config, so behavior is unchanged
// unless the host supplies its own Authorizer via WithAuthorizer. Any capability
// outside the gated set is allowed (the base read/write caps are ungated).
func flagAuthorizer(cfg *config) Authorizer {
	return AuthorizerFunc(func(_ context.Context, capability string) (bool, error) {
		switch capability {
		case "analytics.admin":
			return cfg.AnalyticsAdmin, nil
		case "analytics.read":
			return cfg.AnalyticsRead || cfg.AnalyticsAdmin, nil
		case "schema.admin":
			return cfg.SchemaAdmin, nil
		case "tenants.admin":
			return cfg.TenantsAdmin, nil
		case "connections.read":
			return cfg.ConnectionsRead, nil
		default:
			return true, nil
		}
	})
}

// requireCap gates a handler on a capability via the configured Authorizer.
// Denied → 403; an authorizer error → 500 (fail-closed: never allow on error).
func (c *adminController) requireCap(ctx forge.Context, capability string) error {
	allowed, err := c.ext.cfg.Authorizer.Authorize(ctx.Request().Context(), capability)
	if err != nil {
		// Log the real cause server-side (a flapping authz backend otherwise
		// surfaces only as a bare 500), but return an opaque message so no
		// backend internals leak to the client.
		slog.Error("fabriq.adminapi.authz check failed", "capability", capability, "error", err)
		return forge.InternalError(errors.New("authorization check failed"))
	}
	if !allowed {
		return forge.Forbidden(capability + " not permitted")
	}
	return nil
}
