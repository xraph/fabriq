package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/migrations"
)

// registerTenantRoutes wires the db-per-tenant management surface (catalog
// mode). All handlers gate on requireTenantsAdmin — the HTTP twin of the
// `fabriq tenant` CLI.
func (c *adminController) registerTenantRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := c.ext.cfg.RouteOptions

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.list"),
		forge.WithSummary("List catalog tenants (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.GET(base+"/tenants", c.handleTenantList, listOpts...); err != nil {
		return err
	}

	provisionOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.provision"),
		forge.WithSummary("Provision a tenant database async (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.POST(base+"/tenants", c.handleTenantProvision, provisionOpts...); err != nil {
		return err
	}

	// Static routes under /tenants/... are registered BEFORE the /tenants/:id
	// param route below so they are not shadowed by it.
	migrateAllOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.migrateAll"),
		forge.WithSummary("Migrate every active tenant database to head, async (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.POST(base+"/tenants/migrate-all", c.handleTenantMigrateAll, migrateAllOpts...); err != nil {
		return err
	}

	jobOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.job"),
		forge.WithSummary("Poll a tenant provision/migrate-all job (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.GET(base+"/tenants/jobs/:id", c.handleTenantJob, jobOpts...); err != nil {
		return err
	}

	jobStreamOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.jobStream"),
		forge.WithSummary("Stream a tenant provision/migrate-all job via SSE (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.GET(base+"/tenants/jobs/:id/stream", c.handleTenantJobStream, jobStreamOpts...); err != nil {
		return err
	}

	getOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.get"),
		forge.WithSummary("Get a tenant's catalog entry (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.GET(base+"/tenants/:id", c.handleTenantGet, getOpts...); err != nil {
		return err
	}

	suspendOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.suspend"),
		forge.WithSummary("Suspend a tenant (route off; catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	if err := r.POST(base+"/tenants/:id/suspend", c.handleTenantSuspend, suspendOpts...); err != nil {
		return err
	}

	resumeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.resume"),
		forge.WithSummary("Resume a suspended tenant (catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Tenants"),
	}, opts...)
	return r.POST(base+"/tenants/:id/resume", c.handleTenantResume, resumeOpts...)
}

// tenantView is the JSON projection of a catalog.Entry.
type tenantView struct {
	TenantID  string `json:"tenantId"`
	ClusterID string `json:"clusterId"`
	Database  string `json:"database"`
	State     string `json:"state"`
	Version   string `json:"version"`
}

func toTenantView(e catalog.Entry) tenantView {
	return tenantView{
		TenantID:  e.TenantID,
		ClusterID: e.ClusterID,
		Database:  e.Database,
		State:     string(e.State),
		Version:   e.Version,
	}
}

// handleTenantList serves GET {BasePath}/tenants — pages through the entire
// catalog and returns every tenant entry.
func (c *adminController) handleTenantList(ctx forge.Context) error {
	if _, err := c.requireTenantsAdmin(ctx); err != nil {
		return err
	}
	stores := c.ext.resolveStores()
	if stores == nil || stores.Catalog == nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{
			"error": "tenant management requires catalog mode (db-per-tenant)",
		})
	}
	cat := stores.Catalog

	var out []tenantView
	cursor := catalog.Cursor("")
	for {
		page, next, err := cat.List(ctx.Request().Context(), cursor, 200)
		if err != nil {
			return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		for _, e := range page {
			out = append(out, toTenantView(e))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return ctx.JSON(http.StatusOK, map[string]any{"tenants": out})
}

// handleTenantGet serves GET {BasePath}/tenants/:id.
func (c *adminController) handleTenantGet(ctx forge.Context) error {
	if _, err := c.requireTenantsAdmin(ctx); err != nil {
		return err
	}
	id := ctx.Param("id")
	if !tenant.Valid(id) {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
	}
	stores := c.ext.resolveStores()
	if stores == nil || stores.Catalog == nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{
			"error": "tenant management requires catalog mode (db-per-tenant)",
		})
	}
	e, err := stores.Catalog.Get(ctx.Request().Context(), id)
	if fabriqerr.CodeOf(err) == fabriqerr.CodeNotFound {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such tenant"})
	}
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return ctx.JSON(http.StatusOK, toTenantView(e))
}

// handleTenantSuspend serves POST {BasePath}/tenants/:id/suspend.
func (c *adminController) handleTenantSuspend(ctx forge.Context) error {
	return c.tenantLifecycle(ctx, func(p *provision.Provisioner, id string) (catalog.Entry, error) {
		return p.Suspend(ctx.Request().Context(), id)
	})
}

// handleTenantResume serves POST {BasePath}/tenants/:id/resume.
func (c *adminController) handleTenantResume(ctx forge.Context) error {
	return c.tenantLifecycle(ctx, func(p *provision.Provisioner, id string) (catalog.Entry, error) {
		return p.Resume(ctx.Request().Context(), id)
	})
}

// tenantLifecycle is the shared gate + error-mapping path for the
// suspend/resume handlers: validate the tenant id, run op against the
// gated Provisioner, and map the typed error to an HTTP status.
func (c *adminController) tenantLifecycle(ctx forge.Context, op func(p *provision.Provisioner, id string) (catalog.Entry, error)) error {
	p, err := c.requireTenantsAdmin(ctx)
	if err != nil {
		return err
	}
	id := ctx.Param("id")
	if !tenant.Valid(id) {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
	}

	e, opErr := op(p, id)
	switch fabriqerr.CodeOf(opErr) {
	case fabriqerr.CodeNotFound:
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such tenant"})
	case fabriqerr.CodeConstraintViolation:
		return ctx.JSON(http.StatusConflict, map[string]string{"error": opErr.Error()})
	}
	if opErr != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": opErr.Error()})
	}
	return ctx.JSON(http.StatusOK, toTenantView(e))
}

// tenantJob is one async tenant-provisioning or fleet-migrate-all run.
type tenantJob struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`  // "provision" | "migrate-all"
	State     string            `json:"state"` // "running" | "done" | "failed"
	TenantID  string            `json:"tenantId,omitempty"`
	Entry     *tenantView       `json:"entry,omitempty"`
	Report    *provision.Report `json:"report,omitempty"`
	Error     string            `json:"error,omitempty"`
	StartedAt time.Time         `json:"startedAt"`
	EndedAt   *time.Time        `json:"endedAt,omitempty"`
}

// tenantJobs is an in-memory async job registry for the tenant-provisioning
// and fleet migrate-all endpoints, mirroring migrationJobs. Unlike
// migrationJobs it is not single-flight: multiple tenant provisions (and a
// concurrent migrate-all) may legitimately run at once.
type tenantJobs struct {
	mu   sync.Mutex
	jobs map[string]*tenantJob
}

func newTenantJobs() *tenantJobs { return &tenantJobs{jobs: map[string]*tenantJob{}} }

// start records a running job and runs it in a detached goroutine (so it
// outlives the request). run returns the terminal Entry/Report to merge into
// the job on success; any non-nil fields on the returned *tenantJob are
// copied onto the tracked job under lock.
func (j *tenantJobs) start(kind, tenantID string, run func() (*tenantJob, error)) *tenantJob {
	job := &tenantJob{ID: event.NewID(), Kind: kind, State: "running", TenantID: tenantID, StartedAt: time.Now()}
	j.mu.Lock()
	j.jobs[job.ID] = job
	j.mu.Unlock()

	go func() {
		res, err := run()
		j.mu.Lock()
		defer j.mu.Unlock()
		now := time.Now()
		job.EndedAt = &now
		if err != nil {
			job.State, job.Error = "failed", err.Error()
			return
		}
		job.State = "done"
		if res != nil {
			job.Entry, job.Report = res.Entry, res.Report
		}
	}()
	return job
}

// provisionBody is the POST {BasePath}/tenants request body.
type provisionBody struct {
	TenantID  string `json:"tenantId"`
	ClusterID string `json:"clusterId"`
}

// handleTenantProvision serves POST {BasePath}/tenants — starts an async
// provisioning job for the given tenant/cluster and returns 202 with a
// pollable jobId. The provisioning itself runs detached from the request
// (context.Background()) so it survives the response being written.
func (c *adminController) handleTenantProvision(ctx forge.Context) error {
	p, err := c.requireTenantsAdmin(ctx)
	if err != nil {
		return err
	}
	var body provisionBody
	if derr := json.NewDecoder(ctx.Request().Body).Decode(&body); derr != nil || body.TenantID == "" || body.ClusterID == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "tenantId and clusterId are required"})
	}
	if !tenant.Valid(body.TenantID) {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
	}
	job := c.tenantJobs.start("provision", body.TenantID, func() (*tenantJob, error) {
		e, perr := p.Provision(context.Background(), body.TenantID, body.ClusterID)
		if perr != nil {
			return nil, perr
		}
		v := toTenantView(e)
		return &tenantJob{Entry: &v}, nil
	})
	return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
}

// handleTenantMigrateAll serves POST {BasePath}/tenants/migrate-all — starts
// an async fleet migration roll (to migrations.HeadVersion()) across every
// active tenant database and returns 202 with a pollable jobId.
func (c *adminController) handleTenantMigrateAll(ctx forge.Context) error {
	p, err := c.requireTenantsAdmin(ctx)
	if err != nil {
		return err
	}
	job := c.tenantJobs.start("migrate-all", "", func() (*tenantJob, error) {
		rep, merr := p.MigrateAll(context.Background(), provision.MigrateAllOpts{TargetVersion: migrations.HeadVersion()})
		if merr != nil {
			return nil, merr
		}
		return &tenantJob{Report: &rep}, nil
	})
	return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
}

// handleTenantJob serves GET {BasePath}/tenants/jobs/:id — poll one job. Not
// gated by requireTenantsAdmin (mirrors handleMigrationJob): job ids are
// unguessable event.NewID() values, so a bare read by id needs no additional
// capability check beyond knowing the id.
func (c *adminController) handleTenantJob(ctx forge.Context) error {
	c.tenantJobs.mu.Lock()
	job, ok := c.tenantJobs.jobs[ctx.Param("id")]
	var snap tenantJob
	if ok {
		snap = *job
	}
	c.tenantJobs.mu.Unlock()
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	return ctx.JSON(http.StatusOK, snap)
}

// handleTenantJobStream serves GET {BasePath}/tenants/jobs/:id/stream — an
// SSE stream of the job's state (a "state" event every ~600ms) until the job
// reaches a terminal state (done|failed) or the client disconnects.
func (c *adminController) handleTenantJobStream(ctx forge.Context) error {
	c.tenantJobs.mu.Lock()
	job, ok := c.tenantJobs.jobs[ctx.Param("id")]
	c.tenantJobs.mu.Unlock()
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return err
	}
	reqCtx := ctx.Request().Context()
	ticker := time.NewTicker(600 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.tenantJobs.mu.Lock()
		snap := *job // copy under lock
		c.tenantJobs.mu.Unlock()

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
