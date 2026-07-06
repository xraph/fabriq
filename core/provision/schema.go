package provision

import (
	"context"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/pathctx"
)

// SchemaProvisioner drives tenant lifecycles for schema-per-tenant
// consolidation mode: many tenants share one consolidation database, each
// isolated by a schema (spec 2026-07-03 schema design, S6). It mirrors
// Provisioner's idempotent, resumable state machine
// (pending → creating → migrating → active) but the physical "creating" step
// bootstraps the consolidation database once and creates the tenant schema,
// and "migrating" runs the chain inside that schema.
//
// Placement is explicit: the caller names both the cluster and the
// consolidation database (S5); the schema is derived deterministically from
// the tenant id and recorded in Entry.Schema.
type SchemaProvisioner struct {
	cat          catalog.Catalog
	ops          SchemaClusterOps
	sharedSchema string
	// schemaName derives the tenant schema; overridable in tests.
	schemaName func(tenantID string) (string, error)
}

// NewSchemaProvisioner builds a schema-mode provisioner. An empty
// sharedSchema defaults to "fabriq_shared".
func NewSchemaProvisioner(cat catalog.Catalog, ops SchemaClusterOps, sharedSchema string) *SchemaProvisioner {
	if sharedSchema == "" {
		sharedSchema = "fabriq_shared"
	}
	return &SchemaProvisioner{
		cat:          cat,
		ops:          ops,
		sharedSchema: sharedSchema,
		schemaName:   pathctx.SchemaForTenant,
	}
}

// Provision creates (or resumes) a tenant's schema in the named consolidation
// database and returns the active catalog entry. Idempotent: an already-active
// tenant returns unchanged; a half-provisioned one resumes (both physical
// steps are idempotent). A CAS conflict surfaces as CodeVersionConflict.
func (p *SchemaProvisioner) Provision(ctx context.Context, tenantID, clusterID, database string) (catalog.Entry, error) {
	if tenantID == "" || clusterID == "" || database == "" {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeInvalidInput,
			"schema provisioning requires a tenant id, a cluster id and a consolidation database.")
	}
	schema, err := p.schemaName(tenantID)
	if err != nil {
		return catalog.Entry{}, err
	}

	entry, err := p.cat.Get(ctx, tenantID)
	switch {
	case fabriqerr.CodeOf(err) == fabriqerr.CodeNotFound:
		entry, err = p.cat.Put(ctx, catalog.Entry{
			TenantID:  tenantID,
			ClusterID: clusterID,
			Database:  database,
			Schema:    schema,
			State:     catalog.StatePending,
		})
		if err != nil {
			return catalog.Entry{}, err
		}
	case err != nil:
		return catalog.Entry{}, err
	case entry.ClusterID != clusterID || entry.Database != database:
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeConstraintViolation,
			"tenant is already placed on another cluster/database (moves are a separate operation).",
			fabriqerr.WithEntity("tenant", tenantID))
	case entry.State == catalog.StateActive:
		return entry, nil // already provisioned
	case entry.State == catalog.StateSuspended:
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeUnavailable,
			"tenant is suspended; resume it instead of re-provisioning.",
			fabriqerr.WithEntity("tenant", tenantID))
	}

	// creating → bootstrap the consolidation database (once) + create schema.
	if entry, err = transitionEntry(ctx, p.cat, entry, catalog.StateCreating); err != nil {
		return catalog.Entry{}, err
	}
	if opErr := p.ops.EnsureBootstrap(ctx, entry.ClusterID, entry.Database, p.sharedSchema); opErr != nil {
		return catalog.Entry{}, failEntry(ctx, p.cat, entry, "bootstrap", opErr)
	}
	if opErr := p.ops.CreateSchema(ctx, entry.ClusterID, entry.Database, entry.Schema); opErr != nil {
		return catalog.Entry{}, failEntry(ctx, p.cat, entry, "create schema", opErr)
	}

	// migrating → run the chain inside the schema (idempotent).
	if entry, err = transitionEntry(ctx, p.cat, entry, catalog.StateMigrating); err != nil {
		return catalog.Entry{}, err
	}
	version, opErr := p.ops.MigrateSchema(ctx, entry.ClusterID, entry.Database, entry.Schema, p.sharedSchema)
	if opErr != nil {
		return catalog.Entry{}, failEntry(ctx, p.cat, entry, "migrate", opErr)
	}

	// active — the last step; a crash before this leaves a resumable row.
	entry.Version = version
	return transitionEntry(ctx, p.cat, entry, catalog.StateActive)
}

// Suspend routes a tenant off (takes effect within the directory TTL).
func (p *SchemaProvisioner) Suspend(ctx context.Context, tenantID string) (catalog.Entry, error) {
	entry, err := p.cat.Get(ctx, tenantID)
	if err != nil {
		return catalog.Entry{}, err
	}
	return transitionEntry(ctx, p.cat, entry, catalog.StateSuspended)
}

// Resume re-activates a suspended tenant.
func (p *SchemaProvisioner) Resume(ctx context.Context, tenantID string) (catalog.Entry, error) {
	entry, err := p.cat.Get(ctx, tenantID)
	if err != nil {
		return catalog.Entry{}, err
	}
	if entry.State != catalog.StateSuspended {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeConstraintViolation,
			"only suspended tenants can be resumed.",
			fabriqerr.WithEntity("tenant", tenantID))
	}
	return transitionEntry(ctx, p.cat, entry, catalog.StateActive)
}

// MigrateAll upgrades every active tenant's schema in bounded batches,
// recording each new version — the schema-mode fleet roller.
func (p *SchemaProvisioner) MigrateAll(ctx context.Context, opts MigrateAllOpts) (Report, error) {
	return fleetMigrate(ctx, p.cat, opts, func(e catalog.Entry) (string, error) {
		return p.ops.MigrateSchema(ctx, e.ClusterID, e.Database, e.Schema, p.sharedSchema)
	})
}
