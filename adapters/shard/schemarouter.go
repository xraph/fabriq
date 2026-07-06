package shard

import (
	"context"

	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/tenant"
)

// SchemaRouter wraps a Router and stamps the tenant's consolidation-mode
// schema (pathctx) onto the routing context, so the adapter's per-tx stamp
// sets search_path to the tenant's schema within the shared consolidation
// database. It is wired only in schema isolation (Config.Catalog.Isolation
// == "schema"); the shard it wraps (a DynamicSet over a consolidation
// database) is shared by many tenants, and this decorator is the one seam
// that distinguishes them per request.
//
// The schema is derived deterministically from the tenant id
// (pathctx.SchemaForTenant), matching what the provisioner persisted in
// Entry.Schema — so routing needs no extra catalog lookup on the hot path.
type SchemaRouter struct{ inner Router }

// NewSchemaRouter builds the schema-stamping decorator over inner.
func NewSchemaRouter(inner Router) *SchemaRouter { return &SchemaRouter{inner: inner} }

// Acquire implements Router: resolve the ctx tenant, then stamp its schema.
func (r *SchemaRouter) Acquire(ctx context.Context) (Shard, context.Context, func(), error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Shard{}, nil, nil, err
	}
	return r.AcquireFor(ctx, tid)
}

// AcquireFor implements Router: acquire the consolidation shard through the
// inner router, then return a context carrying the tenant's search_path.
func (r *SchemaRouter) AcquireFor(ctx context.Context, tenantID string) (Shard, context.Context, func(), error) {
	schema, err := pathctx.SchemaForTenant(tenantID)
	if err != nil {
		return Shard{}, nil, nil, err
	}
	sh, sctx, release, err := r.inner.AcquireFor(ctx, tenantID)
	if err != nil {
		return Shard{}, nil, nil, err
	}
	sctx, err = pathctx.WithSearchPath(sctx, schema)
	if err != nil {
		release()
		return Shard{}, nil, nil, err
	}
	return sh, sctx, release, nil
}

var _ Router = (*SchemaRouter)(nil)
