// Package provision is the db-per-tenant onboarding control plane (spec
// 2026-07-03, D4/D7): an idempotent, resumable state machine that creates
// a tenant's dedicated database, migrates it to head, and flips the
// catalog entry active — plus the fleet migration roller that upgrades
// every tenant database in bounded batches.
//
// Provisioning is EXPLICIT (a CLI verb / admin endpoint), never implicit
// on first write, and fabriq never drops a database: offboarding is
// Suspend (route-off); the physical drop is a human decision.
package provision

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// ClusterOps performs the physical work on a Postgres cluster. Both
// operations MUST be idempotent — the state machine's resumability
// (crash between any two steps, re-run converges) rests on it.
type ClusterOps interface {
	// CreateDatabase ensures the database exists on the cluster.
	CreateDatabase(ctx context.Context, clusterID, database string) error
	// Migrate runs fabriq's migration chain against the database and
	// returns the resulting head version.
	Migrate(ctx context.Context, clusterID, database string) (version string, err error)
}

// SchemaClusterOps performs the physical work for schema-per-tenant
// consolidation mode: many tenants share one consolidation database, each
// isolated by a schema. Every operation MUST be idempotent — the
// SchemaProvisioner's resumability rests on it.
type SchemaClusterOps interface {
	// EnsureBootstrap prepares a consolidation database ONCE: creates the
	// shared schema and its extensions (pgvector/postgis) so every tenant
	// schema resolves their types via search_path. Idempotent per database.
	EnsureBootstrap(ctx context.Context, clusterID, database, sharedSchema string) error
	// CreateSchema ensures a tenant's schema exists in the consolidation
	// database.
	CreateSchema(ctx context.Context, clusterID, database, schema string) error
	// MigrateSchema runs fabriq's migration chain inside a tenant schema
	// (bare-named DDL + grove_migrations land there via search_path) and
	// returns the resulting head version.
	MigrateSchema(ctx context.Context, clusterID, database, schema, sharedSchema string) (version string, err error)
}

// Provisioner drives tenant lifecycles against the catalog.
type Provisioner struct {
	cat catalog.Catalog
	ops ClusterOps
	// databaseName derives the tenant database name (default "fabriq_" +
	// tenant id, which is already restricted to a safe identifier set by
	// tenant.Valid at the API boundary).
	databaseName func(tenantID string) string
}

// New builds a Provisioner.
func New(cat catalog.Catalog, ops ClusterOps) *Provisioner {
	return &Provisioner{
		cat: cat,
		ops: ops,
		databaseName: func(tenantID string) string {
			return "fabriq_" + tenantID
		},
	}
}

// Provision creates (or resumes) a tenant's dedicated database and returns
// the active catalog entry. Idempotent: an already-active tenant returns
// its entry unchanged; a half-provisioned one resumes from its state (both
// physical steps are idempotent, so the states are progress markers, not
// resume points). A CAS conflict means another provisioner holds the
// tenant — surfaced as CodeVersionConflict for the caller to observe/retry.
func (p *Provisioner) Provision(ctx context.Context, tenantID, clusterID string) (catalog.Entry, error) {
	if tenantID == "" || clusterID == "" {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeInvalidInput,
			"provisioning requires a tenant id and a cluster id.")
	}

	entry, err := p.cat.Get(ctx, tenantID)
	switch {
	case fabriqerr.CodeOf(err) == fabriqerr.CodeNotFound:
		entry, err = p.cat.Put(ctx, catalog.Entry{
			TenantID:  tenantID,
			ClusterID: clusterID,
			Database:  p.databaseName(tenantID),
			State:     catalog.StatePending,
		})
		if err != nil {
			return catalog.Entry{}, err
		}
	case err != nil:
		return catalog.Entry{}, err
	case entry.ClusterID != clusterID:
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeConstraintViolation,
			"tenant is already placed on another cluster (moves are a separate operation).",
			fabriqerr.WithEntity("tenant", tenantID))
	case entry.State == catalog.StateActive:
		return entry, nil // already provisioned
	case entry.State == catalog.StateSuspended:
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeUnavailable,
			"tenant is suspended; resume it instead of re-provisioning.",
			fabriqerr.WithEntity("tenant", tenantID))
	}

	// creating → CREATE DATABASE (idempotent).
	if entry, err = p.transition(ctx, entry, catalog.StateCreating); err != nil {
		return catalog.Entry{}, err
	}
	if opErr := p.ops.CreateDatabase(ctx, entry.ClusterID, entry.Database); opErr != nil {
		return catalog.Entry{}, p.fail(ctx, entry, "create database", opErr)
	}

	// migrating → run the chain (idempotent).
	if entry, err = p.transition(ctx, entry, catalog.StateMigrating); err != nil {
		return catalog.Entry{}, err
	}
	version, opErr := p.ops.Migrate(ctx, entry.ClusterID, entry.Database)
	if opErr != nil {
		return catalog.Entry{}, p.fail(ctx, entry, "migrate", opErr)
	}

	// active — the last step; a crash before this leaves a resumable row.
	entry.Version = version
	return p.transition(ctx, entry, catalog.StateActive)
}

// Suspend routes a tenant off (takes effect within the directory TTL).
// The database is untouched.
func (p *Provisioner) Suspend(ctx context.Context, tenantID string) (catalog.Entry, error) {
	return p.setState(ctx, tenantID, catalog.StateSuspended)
}

// Resume re-activates a suspended tenant.
func (p *Provisioner) Resume(ctx context.Context, tenantID string) (catalog.Entry, error) {
	entry, err := p.cat.Get(ctx, tenantID)
	if err != nil {
		return catalog.Entry{}, err
	}
	if entry.State != catalog.StateSuspended {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeConstraintViolation,
			"only suspended tenants can be resumed.",
			fabriqerr.WithEntity("tenant", tenantID))
	}
	return p.transition(ctx, entry, catalog.StateActive)
}

func (p *Provisioner) setState(ctx context.Context, tenantID string, s catalog.State) (catalog.Entry, error) {
	entry, err := p.cat.Get(ctx, tenantID)
	if err != nil {
		return catalog.Entry{}, err
	}
	return p.transition(ctx, entry, s)
}

func (p *Provisioner) transition(ctx context.Context, e catalog.Entry, s catalog.State) (catalog.Entry, error) {
	e.State = s
	return p.cat.Put(ctx, e)
}

// fail best-effort marks the entry failed (listable by operators) and
// returns the step error. The CAS may lose to a concurrent provisioner;
// that is fine — someone is making progress.
func (p *Provisioner) fail(ctx context.Context, e catalog.Entry, step string, cause error) error {
	e.State = catalog.StateFailed
	_, _ = p.cat.Put(ctx, e)
	return fabriqerr.New(fabriqerr.CodeUnavailable,
		"tenant provisioning failed.",
		fabriqerr.WithEntity("tenant", e.TenantID),
		fabriqerr.WithCause(fmt.Errorf("%s: %w", step, cause)))
}

// MigrateAllOpts tunes the fleet roller.
type MigrateAllOpts struct {
	// Batch is the number of tenant databases migrated concurrently
	// (default 8).
	Batch int
	// MaxFailures stops the roll after this many failed tenants
	// (default 3). The roll is always safe to re-run.
	MaxFailures int
	// TargetVersion skips tenants already recorded at it ("" = always run;
	// the migration chain itself is idempotent).
	TargetVersion string
}

// TenantResult is one tenant's outcome in a fleet roll.
type TenantResult struct {
	TenantID string `json:"tenantId"`
	Version  string `json:"version,omitempty"`
	Skipped  bool   `json:"skipped,omitempty"`
	Err      string `json:"error,omitempty"`
}

// Report summarizes a fleet roll.
type Report struct {
	Migrated int            `json:"migrated"`
	Skipped  int            `json:"skipped"`
	Failed   int            `json:"failed"`
	Results  []TenantResult `json:"results"`
}

// MigrateAll walks the catalog and migrates every active tenant database,
// Batch at a time, stopping once MaxFailures tenants have failed. Each
// success records the tenant's new version in the catalog.
func (p *Provisioner) MigrateAll(ctx context.Context, opts MigrateAllOpts) (Report, error) {
	if opts.Batch <= 0 {
		opts.Batch = 8
	}
	if opts.MaxFailures <= 0 {
		opts.MaxFailures = 3
	}

	var entries []catalog.Entry
	cursor := catalog.Cursor("")
	for {
		page, next, err := p.cat.List(ctx, cursor, 500)
		if err != nil {
			return Report{}, err
		}
		entries = append(entries, page...)
		if next == "" {
			break
		}
		cursor = next
	}

	var (
		mu       sync.Mutex
		report   Report
		failures int
		sem      = make(chan struct{}, opts.Batch)
		wg       sync.WaitGroup
	)

	for _, e := range entries {
		if e.State != catalog.StateActive {
			continue // suspended/failed/half-provisioned tenants are not rolled
		}
		if opts.TargetVersion != "" && e.Version == opts.TargetVersion {
			mu.Lock()
			report.Skipped++
			report.Results = append(report.Results, TenantResult{TenantID: e.TenantID, Version: e.Version, Skipped: true})
			mu.Unlock()
			continue
		}
		mu.Lock()
		over := failures >= opts.MaxFailures
		mu.Unlock()
		if over {
			break
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(e catalog.Entry) {
			defer wg.Done()
			defer func() { <-sem }()

			version, err := p.ops.Migrate(ctx, e.ClusterID, e.Database)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures++
				report.Failed++
				report.Results = append(report.Results, TenantResult{TenantID: e.TenantID, Err: err.Error()})
				return
			}
			report.Migrated++
			report.Results = append(report.Results, TenantResult{TenantID: e.TenantID, Version: version})
			// Record the observed version (best-effort CAS; a concurrent
			// writer just means fresher state already landed).
			e.Version = version
			if updated, putErr := p.cat.Put(ctx, e); putErr == nil {
				_ = updated
			}
		}(e)
	}
	wg.Wait()

	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].TenantID < report.Results[j].TenantID })
	if report.Failed >= opts.MaxFailures {
		return report, fabriqerr.New(fabriqerr.CodeUnavailable,
			"fleet migration stopped at the failure budget.",
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{
				"failed": fmt.Sprint(report.Failed),
			}}))
	}
	return report, nil
}
