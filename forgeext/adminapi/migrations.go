package adminapi

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/event"
)

// migrationItem mirrors one migration's status in camelCase.
type migrationItem struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Group     string `json:"group"`
	Comment   string `json:"comment"`
	Applied   bool   `json:"applied"`
	AppliedAt string `json:"appliedAt,omitempty"`
}

// migrationGroup is one migration group's applied/pending status.
type migrationGroup struct {
	Name    string          `json:"name"`
	Applied []migrationItem `json:"applied"`
	Pending []migrationItem `json:"pending"`
}

// migrationStatusResponse is the payload for GET {BasePath}/migrations.
type migrationStatusResponse struct {
	Groups []migrationGroup `json:"groups"`
}

// registerMigrationRoutes wires the read-only migration status route. This
// endpoint is always available (no WithSchemaAdmin gate) — it only reports
// state, it never executes migrations.
func (c *adminController) registerMigrationRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.migrations.status"),
		forge.WithSummary("List applied + pending migrations"),
		forge.WithTags("Fabriq", "Admin"),
	}, c.ext.cfg.RouteOptions...)
	if err := r.GET(base+"/migrations", c.handleMigrationStatus, opts...); err != nil {
		return err
	}
	// Execution (gated by WithSchemaAdmin, checked in the handlers) + job status.
	if err := r.POST(base+"/migrations/up", c.handleMigrateUp, opts...); err != nil {
		return err
	}
	if err := r.POST(base+"/migrations/down", c.handleMigrateDown, opts...); err != nil {
		return err
	}
	return r.GET(base+"/migrations/jobs/:id", c.handleMigrationJob, opts...)
}

// migrationJob is one async migration run (up or down).
type migrationJob struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`  // "up" | "down"
	State     string    `json:"state"` // "running" | "done" | "failed"
	Names     []string  `json:"names,omitempty"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt,omitempty"`
}

// migrationJobs is a single-flight in-memory job registry. Migrations serialize
// under grove's advisory lock, so one running job at a time is the model.
type migrationJobs struct {
	mu      sync.Mutex
	jobs    map[string]*migrationJob
	running bool
}

func newMigrationJobs() *migrationJobs { return &migrationJobs{jobs: map[string]*migrationJob{}} }

// startMigrationJob records a running job and runs it in a detached goroutine
// (so it outlives the request). Returns an error if a run is already in flight.
func (c *adminController) startMigrationJob(kind string, run func(context.Context) (*forge.MigrationResult, error)) (*migrationJob, error) {
	c.jobs.mu.Lock()
	if c.jobs.running {
		c.jobs.mu.Unlock()
		return nil, fmt.Errorf("another migration run is in progress")
	}
	job := &migrationJob{ID: event.NewID(), Kind: kind, State: "running", StartedAt: time.Now()}
	c.jobs.jobs[job.ID] = job
	c.jobs.running = true
	c.jobs.mu.Unlock()

	go func() {
		res, err := run(context.Background())
		c.jobs.mu.Lock()
		defer c.jobs.mu.Unlock()
		job.EndedAt = time.Now()
		if err != nil {
			job.State, job.Error = "failed", err.Error()
		} else {
			job.State = "done"
			if res != nil {
				job.Names = res.Names
			}
		}
		c.jobs.running = false
	}()
	return job, nil
}

// handleMigrateUp serves POST {BasePath}/migrations/up — gated. Runs all pending
// migrations in a background job; returns 202 with a pollable jobId.
func (c *adminController) handleMigrateUp(ctx forge.Context) error {
	return c.startMigrationRun(ctx, "up", c.migrateFn())
}

// handleMigrateDown serves POST {BasePath}/migrations/down — gated. Rolls back
// the last applied batch in a background job.
func (c *adminController) handleMigrateDown(ctx forge.Context) error {
	return c.startMigrationRun(ctx, "down", c.rollbackFn())
}

// startMigrationRun is the shared gate + nil-guard + start path for up/down.
func (c *adminController) startMigrationRun(ctx forge.Context, kind string, run func(context.Context) (*forge.MigrationResult, error)) error {
	if err := c.requireSchemaAdmin(ctx); err != nil {
		return err
	}
	if c.ext.parent == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{
			"error": "migration execution unavailable: no parent fabriq extension configured",
		})
	}
	job, err := c.startMigrationJob(kind, run)
	if err != nil {
		return ctx.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	}
	return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
}

// migrateFn / rollbackFn adapt the parent's Migrate/Rollback to the job runner
// signature (they are nil-safe only after the parent nil-guard above).
func (c *adminController) migrateFn() func(context.Context) (*forge.MigrationResult, error) {
	return func(ctx context.Context) (*forge.MigrationResult, error) { return c.ext.parent.Migrate(ctx) }
}

func (c *adminController) rollbackFn() func(context.Context) (*forge.MigrationResult, error) {
	return func(ctx context.Context) (*forge.MigrationResult, error) { return c.ext.parent.Rollback(ctx) }
}

// handleMigrationJob serves GET {BasePath}/migrations/jobs/:id — poll one job.
func (c *adminController) handleMigrationJob(ctx forge.Context) error {
	id := ctx.Param("id")
	c.jobs.mu.Lock()
	job, ok := c.jobs.jobs[id]
	c.jobs.mu.Unlock()
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	return ctx.JSON(http.StatusOK, job)
}

// handleMigrationStatus serves GET {BasePath}/migrations. Returns 501 when
// there is no parent forgeext.Extension to resolve a migration target from
// (e.g. the fake-backed test harness) or when the parent itself fails to
// report status (no DSN/grove.DB configured).
func (c *adminController) handleMigrationStatus(ctx forge.Context) error {
	if c.ext.parent == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{
			"error": "migration status unavailable: no parent fabriq extension configured",
		})
	}
	groups, err := c.ext.parent.MigrationStatus(ctx.Request().Context())
	if err != nil {
		// No migration target on the fake/unconfigured backend → 501.
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": err.Error()})
	}
	out := migrationStatusResponse{Groups: make([]migrationGroup, 0, len(groups))}
	for _, g := range groups {
		mg := migrationGroup{
			Name:    g.Name,
			Applied: make([]migrationItem, 0, len(g.Applied)),
			Pending: make([]migrationItem, 0, len(g.Pending)),
		}
		for _, m := range g.Applied {
			mg.Applied = append(mg.Applied, migrationInfoTo(m))
		}
		for _, m := range g.Pending {
			mg.Pending = append(mg.Pending, migrationInfoTo(m))
		}
		out.Groups = append(out.Groups, mg)
	}
	return ctx.JSON(http.StatusOK, out)
}

// migrationInfoTo converts a forge.MigrationInfo into the camelCase wire type.
func migrationInfoTo(m *forge.MigrationInfo) migrationItem {
	return migrationItem{
		Name:      m.Name,
		Version:   m.Version,
		Group:     m.Group,
		Comment:   m.Comment,
		Applied:   m.Applied,
		AppliedAt: m.AppliedAt,
	}
}
