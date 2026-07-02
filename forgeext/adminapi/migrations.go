package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/subscribe"
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
	if err := r.GET(base+"/migrations/jobs/:id", c.handleMigrationJob, opts...); err != nil {
		return err
	}
	if err := r.GET(base+"/migrations/jobs/:id/stream", c.handleMigrationJobStream, opts...); err != nil {
		return err
	}
	return r.POST(base+"/migrations/scaffold", c.handleMigrationScaffold, opts...)
}

// scaffoldNameRe / scaffoldVersionRe validate the scaffold inputs: a lowercase
// snake_case migration name and an all-digit version (grove uses a timestamp).
var scaffoldNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
var scaffoldVersionRe = regexp.MustCompile(`^[0-9]+$`)

// scaffoldVarName turns "add_widget" into "AddWidget" for the migration var name.
func scaffoldVarName(name string) string {
	parts := strings.Split(name, "_")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

// scaffoldStmtBlock renders the body of an execAll([]string{...}) block: one
// %q-quoted statement per line at 3-tab indent. Blank/whitespace-only entries
// are dropped; when nothing is left it emits the TODO placeholder comment so the
// generated file still compiles and reads like the hand-written migrations.
func scaffoldStmtBlock(stmts []string, todo string) string {
	var kept []string
	for _, s := range stmts {
		if s = strings.TrimSpace(s); s != "" {
			kept = append(kept, s)
		}
	}
	var b strings.Builder
	if len(kept) == 0 {
		fmt.Fprintf(&b, "\t\t\t%s\n", todo)
		return b.String()
	}
	for _, s := range kept {
		fmt.Fprintf(&b, "\t\t\t%q,\n", s)
	}
	return b.String()
}

// renderMigrationScaffold generates a grove Go migration-file skeleton (the same
// shape as migrations/0001_outbox.go). It executes nothing — it returns text for
// a developer to save into migrations/ and register in module.go. When up/down
// statements are supplied they are emitted inside the execAll blocks so the file
// is save-ready with no further editing; when empty, TODO placeholders are left.
// Returns an error on an invalid name/version.
func renderMigrationScaffold(name, version string, up, down []string) (filename, content string, err error) {
	if !scaffoldNameRe.MatchString(name) {
		return "", "", fmt.Errorf("invalid migration name %q (want lowercase snake_case)", name)
	}
	if !scaffoldVersionRe.MatchString(version) {
		return "", "", fmt.Errorf("invalid version %q (want an all-digit timestamp, e.g. 202607010001)", version)
	}
	filename = name + ".go"
	content = fmt.Sprintf(`package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// TODO: describe this migration. Register migration%[1]s in migrations/module.go
// (Group().MustRegister(...)) in version order.
var migration%[1]s = &migrate.Migration{
	Name:    %[2]q,
	Version: %[3]q,
	Comment: "TODO: describe this migration",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
%[4]s		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
%[5]s		})
	},
}
`, scaffoldVarName(name), name, version,
		scaffoldStmtBlock(up, `// TODO: forward DDL, e.g. "CREATE TABLE IF NOT EXISTS ...".`),
		scaffoldStmtBlock(down, `// TODO: reverse DDL, e.g. "DROP TABLE IF EXISTS ...".`))
	return filename, content, nil
}

// scaffoldRequest is the POST body for the scaffold endpoint. up/down are
// optional forward/reverse DDL statements (one per element); when omitted the
// generated file carries TODO placeholders.
type scaffoldRequest struct {
	Name    string   `json:"name"`
	Version string   `json:"version"`
	Up      []string `json:"up,omitempty"`
	Down    []string `json:"down,omitempty"`
}

// handleMigrationScaffold serves POST {BasePath}/migrations/scaffold — gated.
// Returns generated Go text; it never touches the database or the filesystem.
func (c *adminController) handleMigrationScaffold(ctx forge.Context) error {
	if err := c.requireSchemaAdmin(ctx); err != nil {
		return err
	}
	var req scaffoldRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid request body: " + err.Error())
	}
	filename, content, err := renderMigrationScaffold(
		strings.TrimSpace(req.Name), strings.TrimSpace(req.Version), req.Up, req.Down)
	if err != nil {
		return forge.BadRequest(err.Error())
	}
	return ctx.JSON(http.StatusOK, map[string]string{"filename": filename, "content": content})
}

// migrationJob is one async migration run (up or down).
type migrationJob struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`  // "up" | "down"
	State     string    `json:"state"` // "running" | "done" | "failed"
	Names     []string  `json:"names,omitempty"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time  `json:"startedAt"`
	EndedAt   *time.Time `json:"endedAt,omitempty"` // pointer so it is truly omitted while running
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
		now := time.Now()
		job.EndedAt = &now
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

// handleMigrationJobStream serves GET {BasePath}/migrations/jobs/:id/stream — an
// SSE stream of the job's state (a "state" event every ~500ms) until the job
// reaches a terminal state (done|failed) or the client disconnects.
func (c *adminController) handleMigrationJobStream(ctx forge.Context) error {
	id := ctx.Param("id")
	c.jobs.mu.Lock()
	job, ok := c.jobs.jobs[id]
	c.jobs.mu.Unlock()
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return err
	}
	reqCtx := ctx.Request().Context()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.jobs.mu.Lock()
		snap := *job // copy under lock
		c.jobs.mu.Unlock()

		// Bound each write so a stalled client cannot wedge the goroutine.
		_ = sse.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if werr := sse.WriteEvent(snap.ID, "state", snap); werr != nil {
			return nil // client gone
		}
		if snap.State != "running" {
			return nil // terminal — final event already sent
		}
		select {
		case <-reqCtx.Done():
			return nil
		case <-ticker.C:
		}
	}
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
