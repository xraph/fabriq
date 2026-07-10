package adminapi

import (
	"context"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/analytics"
)

// registerAnalyticsRoutes wires the cross-tenant analytics sink surface:
// backfill / reproject / reconcile / purge (POST), a status check (GET), and
// the async job poll+stream endpoints. All gate on the
// analytics.admin/analytics.read capabilities — see requireAnalyticsAdmin.
//
// Single-tenant ops run synchronously; a fleet-wide op ("all") may set
// "async": true to run as a background job (202 + jobId), polled at
// /analytics/jobs/:id (or streamed via SSE at .../stream) — for fleets large
// enough to exceed an HTTP timeout.
func (c *adminController) registerAnalyticsRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := c.ext.cfg.RouteOptions

	backfillOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.backfill"),
		forge.WithSummary("Backfill the analytics sink from tenant snapshots (synchronous)"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.POST(base+"/analytics/backfill", c.handleAnalyticsBackfill, backfillOpts...); err != nil {
		return err
	}

	reconcileOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.reconcile"),
		forge.WithSummary("Detect and heal analytics rows missing/stale vs the source of truth"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.POST(base+"/analytics/reconcile", c.handleAnalyticsReconcile, reconcileOpts...); err != nil {
		return err
	}

	jobOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.job"),
		forge.WithSummary("Poll an async analytics bulk-op job"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.GET(base+"/analytics/jobs/:id", c.handleAnalyticsJob, jobOpts...); err != nil {
		return err
	}
	if err := r.GET(base+"/analytics/jobs/:id/stream", c.handleAnalyticsJobStream, jobOpts...); err != nil {
		return err
	}

	purgeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.purge"),
		forge.WithSummary("Erase one tenant's data from the analytics sink (offboarding / erasure)"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.POST(base+"/analytics/purge", c.handleAnalyticsPurge, purgeOpts...); err != nil {
		return err
	}

	reprojectOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.reproject"),
		forge.WithSummary("Re-apply the current redaction allow-list to already-stored rows"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.POST(base+"/analytics/reproject", c.handleAnalyticsReproject, reprojectOpts...); err != nil {
		return err
	}

	queryOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.query"),
		forge.WithSummary("Run a read-only SQL query against the analytics sink"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	if err := r.POST(base+"/analytics/query", c.handleAnalyticsQuery, queryOpts...); err != nil {
		return err
	}

	statusOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.analytics.status"),
		forge.WithSummary("Report whether the analytics sink is configured"),
		forge.WithTags("Fabriq", "Admin", "Analytics"),
	}, opts...)
	return r.GET(base+"/analytics/status", c.handleAnalyticsStatus, statusOpts...)
}

// requireAnalyticsAdmin gates the analytics backfill endpoints. It checks the
// analytics.admin capability opt-in first (403 when off), then resolves the
// Backfiller via the parent extension's Stores (400 when parent/stores are
// absent — e.g. unit tests built directly against a bare Extension — or when
// the analytics sink itself is not configured).
//
// The returned error is a real forge.IHTTPError (not the result of an eager
// ctx.JSON write) so that callers' `if err != nil { return err }` early-return
// actually short-circuits — see requireTenantsAdmin's doc comment for the full
// rationale.
func (c *adminController) requireAnalyticsAdmin(ctx forge.Context) (*analytics.Backfiller, error) {
	if err := c.requireCap(ctx, "analytics.admin"); err != nil {
		return nil, err
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil {
		return nil, forge.BadRequest("analytics backfill requires a started fabriq extension")
	}
	bf, err := c.ext.parent.Stores().AnalyticsBackfiller(c.ext.reg)
	if err != nil {
		return nil, forge.BadRequest(err.Error())
	}
	return bf, nil
}

// requireAnalyticsRead gates the read-only analytics endpoints. It passes when
// EITHER the read grant or the admin grant is enabled (admin implies read),
// and 403s otherwise — mirroring requireAnalyticsAdmin's shape but without the
// Backfiller resolution (reads do not need it).
func (c *adminController) requireAnalyticsRead(ctx forge.Context) error {
	return c.requireCap(ctx, "analytics.read")
}

// backfillRequest is the POST {BasePath}/analytics/backfill request body.
// Exactly one selector must be set: either "tenant" (single-tenant backfill)
// or "all" (fleet backfill, optionally bounded by "concurrency").
type backfillRequest struct {
	Tenant      string `json:"tenant,omitempty"`
	All         bool   `json:"all,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	// Async runs a fleet backfill (All) as a background job, returning 202 +
	// jobId instead of blocking — for fleets large enough to exceed an HTTP
	// timeout. Ignored for a single-tenant backfill.
	Async bool `json:"async,omitempty"`
}

// errString returns err.Error() or "" for nil — for the partial-failure Error
// field on a job result.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// backfillResponse is the payload for a completed backfill, keyed by tenant
// id with the count of analyticized rows.
type backfillResponse struct {
	Counts map[string]int `json:"counts"`
	// Error is set (with HTTP 207 Multi-Status) when a fleet backfill
	// (body.All) partially failed: some tenants succeeded and are reflected
	// in Counts, but at least one tenant errored — see
	// analytics.Backfiller.AllTenants.
	Error string `json:"error,omitempty"`
}

// analyticsStatusResponse is the payload for GET {BasePath}/analytics/status.
type analyticsStatusResponse struct {
	Enabled         bool               `json:"enabled"`
	TenantCount     int                `json:"tenantCount"`
	WorstLagSeconds float64            `json:"worstLagSeconds"`
	TenantsBehind   int                `json:"tenantsBehind"`
	PerTenantLag    map[string]float64 `json:"perTenantLag,omitempty"`
}

// summarizeLag reduces a per-tenant lag map to the worst-case lag (max seconds,
// 0 when empty) and the count of tenants STRICTLY beyond thresholdSecs — the
// same two low-cardinality figures the worker's freshness gauges expose. The
// strict `>` comparison matches the worker's fabriq_analytics_tenants_behind
// gauge (forgeext/worker.go, analyticsLagBehindThreshold) so the dashboard and
// the alert metric agree at the boundary.
func summarizeLag(lag map[string]float64, thresholdSecs float64) (worst float64, behind int) {
	for _, secs := range lag {
		if secs > worst {
			worst = secs
		}
		if secs > thresholdSecs {
			behind++
		}
	}
	return worst, behind
}

// handleAnalyticsBackfill serves POST {BasePath}/analytics/backfill. It runs
// SYNCHRONOUSLY: the response is only written once the replay(s) complete.
func (c *adminController) handleAnalyticsBackfill(ctx forge.Context) error {
	bf, err := c.requireAnalyticsAdmin(ctx)
	if err != nil {
		return err
	}

	var body backfillRequest
	if derr := ctx.BindJSON(&body); derr != nil {
		return forge.BadRequest("invalid request body: " + derr.Error())
	}

	reqCtx := ctx.Request().Context()

	if body.Tenant != "" {
		n, terr := bf.Tenant(reqCtx, body.Tenant)
		if terr != nil {
			return renderError(ctx, terr)
		}
		return ctx.JSON(http.StatusOK, backfillResponse{Counts: map[string]int{body.Tenant: n}})
	}

	if body.All {
		if body.Async {
			conc := body.Concurrency
			job := c.analyticsJobs.start("backfill", func(jctx context.Context) (any, error) {
				tenants, terr := c.ext.parent.Stores().AllTenants(jctx)
				if terr != nil {
					return nil, terr
				}
				counts, aerr := bf.AllTenants(jctx, tenants, conc)
				return backfillResponse{Counts: counts, Error: errString(aerr)}, nil
			})
			return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
		}
		tenants, terr := c.ext.parent.Stores().AllTenants(reqCtx)
		if terr != nil {
			return renderError(ctx, terr)
		}
		// AllTenants runs with bounded concurrency and records one tenant's
		// failure without aborting the others (see analytics.Backfiller.AllTenants),
		// so counts is always meaningful even when aerr is non-nil — surface
		// both rather than discarding the partial results on error.
		counts, aerr := bf.AllTenants(reqCtx, tenants, body.Concurrency)
		if aerr != nil {
			return ctx.JSON(http.StatusMultiStatus, backfillResponse{Counts: counts, Error: aerr.Error()})
		}
		return ctx.JSON(http.StatusOK, backfillResponse{Counts: counts})
	}

	return forge.BadRequest("request body must set either 'tenant' or 'all'")
}

// reprojectRequest is the POST {BasePath}/analytics/reproject request body.
// Exactly one selector: "tenant" (one tenant) or "all" (fleet, bounded by
// "concurrency").
type reprojectRequest struct {
	Tenant      string `json:"tenant,omitempty"`
	All         bool   `json:"all,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Async       bool   `json:"async,omitempty"`
}

// analyticsReconcileRequest is the POST {BasePath}/analytics/reconcile request body.
type analyticsReconcileRequest struct {
	Tenant      string `json:"tenant,omitempty"`
	All         bool   `json:"all,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Async       bool   `json:"async,omitempty"`
}

// analyticsReconcileResponse reports per-tenant drift reports.
type analyticsReconcileResponse struct {
	Reports map[string]analytics.Report `json:"reports"`
	Error   string                      `json:"error,omitempty"`
}

// handleAnalyticsReconcile serves POST {BasePath}/analytics/reconcile: detect
// and heal analytics rows missing/stale vs the source of truth. Gated on
// analytics.admin. Fleet reconcile supports async (202 + jobId).
func (c *adminController) handleAnalyticsReconcile(ctx forge.Context) error {
	if err := c.requireCap(ctx, "analytics.admin"); err != nil {
		return err
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil {
		return forge.BadRequest("analytics reconcile requires a started fabriq extension")
	}
	rc, rerr := c.ext.parent.Stores().AnalyticsReconciler(c.ext.reg)
	if rerr != nil {
		return forge.BadRequest(rerr.Error())
	}
	var body analyticsReconcileRequest
	if derr := ctx.BindJSON(&body); derr != nil {
		return forge.BadRequest("invalid request body: " + derr.Error())
	}
	reqCtx := ctx.Request().Context()

	if body.Tenant != "" {
		rep, terr := rc.Tenant(reqCtx, body.Tenant)
		if terr != nil {
			return renderError(ctx, terr)
		}
		return ctx.JSON(http.StatusOK, analyticsReconcileResponse{Reports: map[string]analytics.Report{body.Tenant: rep}})
	}
	if body.All {
		if body.Async {
			conc := body.Concurrency
			job := c.analyticsJobs.start("reconcile", func(jctx context.Context) (any, error) {
				tenants, terr := c.ext.parent.Stores().AllTenants(jctx)
				if terr != nil {
					return nil, terr
				}
				reports, aerr := rc.AllTenants(jctx, tenants, conc)
				return analyticsReconcileResponse{Reports: reports, Error: errString(aerr)}, nil
			})
			return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
		}
		tenants, terr := c.ext.parent.Stores().AllTenants(reqCtx)
		if terr != nil {
			return renderError(ctx, terr)
		}
		reports, aerr := rc.AllTenants(reqCtx, tenants, body.Concurrency)
		if aerr != nil {
			return ctx.JSON(http.StatusMultiStatus, analyticsReconcileResponse{Reports: reports, Error: aerr.Error()})
		}
		return ctx.JSON(http.StatusOK, analyticsReconcileResponse{Reports: reports})
	}
	return forge.BadRequest("request body must set either 'tenant' or 'all'")
}

// reprojectResponse reports rows rewritten per tenant.
type reprojectResponse struct {
	Counts map[string]int64 `json:"counts"`
	Error  string           `json:"error,omitempty"`
}

// handleAnalyticsReproject serves POST {BasePath}/analytics/reproject: it
// re-applies each marked entity's current redaction allow-list to already-stored
// rows (a retroactive privacy correction). Synchronous; gated on analytics.admin.
func (c *adminController) handleAnalyticsReproject(ctx forge.Context) error {
	if err := c.requireCap(ctx, "analytics.admin"); err != nil {
		return err
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil {
		return forge.BadRequest("analytics reproject requires a started fabriq extension")
	}
	rp, rerr := c.ext.parent.Stores().AnalyticsReprojector(c.ext.reg)
	if rerr != nil {
		return forge.BadRequest(rerr.Error())
	}
	var body reprojectRequest
	if derr := ctx.BindJSON(&body); derr != nil {
		return forge.BadRequest("invalid request body: " + derr.Error())
	}
	reqCtx := ctx.Request().Context()

	if body.Tenant != "" {
		n, terr := rp.Tenant(reqCtx, body.Tenant)
		if terr != nil {
			return renderError(ctx, terr)
		}
		return ctx.JSON(http.StatusOK, reprojectResponse{Counts: map[string]int64{body.Tenant: n}})
	}
	if body.All {
		if body.Async {
			conc := body.Concurrency
			job := c.analyticsJobs.start("reproject", func(jctx context.Context) (any, error) {
				tenants, terr := c.ext.parent.Stores().AllTenants(jctx)
				if terr != nil {
					return nil, terr
				}
				counts, aerr := rp.AllTenants(jctx, tenants, conc)
				return reprojectResponse{Counts: counts, Error: errString(aerr)}, nil
			})
			return ctx.JSON(http.StatusAccepted, map[string]string{"jobId": job.ID})
		}
		tenants, terr := c.ext.parent.Stores().AllTenants(reqCtx)
		if terr != nil {
			return renderError(ctx, terr)
		}
		counts, aerr := rp.AllTenants(reqCtx, tenants, body.Concurrency)
		if aerr != nil {
			return ctx.JSON(http.StatusMultiStatus, reprojectResponse{Counts: counts, Error: aerr.Error()})
		}
		return ctx.JSON(http.StatusOK, reprojectResponse{Counts: counts})
	}
	return forge.BadRequest("request body must set either 'tenant' or 'all'")
}

// purgeRequest is the POST {BasePath}/analytics/purge request body.
type purgeRequest struct {
	Tenant string `json:"tenant"`
}

// purgeResponse reports how many rows the erase removed.
type purgeResponse struct {
	Tenant      string `json:"tenant"`
	RowsDeleted int64  `json:"rowsDeleted"`
}

// handleAnalyticsPurge serves POST {BasePath}/analytics/purge: it hard-deletes
// ALL of one tenant's co-located data (facts, events, watermarks) from the
// analytics sink — the erasure step for tenant offboarding and
// right-to-be-forgotten. Destructive; gated on analytics.admin.
func (c *adminController) handleAnalyticsPurge(ctx forge.Context) error {
	if err := c.requireCap(ctx, "analytics.admin"); err != nil {
		return err
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil || c.ext.parent.Stores().Analytics == nil {
		return forge.BadRequest("analytics purge requires a started fabriq extension with an analytics sink configured")
	}
	var body purgeRequest
	if derr := ctx.BindJSON(&body); derr != nil {
		return forge.BadRequest("invalid request body: " + derr.Error())
	}
	if body.Tenant == "" {
		return forge.BadRequest("request body must set 'tenant'")
	}
	n, perr := c.ext.parent.Stores().Analytics.PurgeTenant(ctx.Request().Context(), body.Tenant)
	if perr != nil {
		return renderError(ctx, perr)
	}
	return ctx.JSON(http.StatusOK, purgeResponse{Tenant: body.Tenant, RowsDeleted: n})
}

// handleAnalyticsStatus serves GET {BasePath}/analytics/status. It reports
// whether the analytics sink is configured and how many tenants are known to
// the catalog, without triggering any backfill work.
func (c *adminController) handleAnalyticsStatus(ctx forge.Context) error {
	if err := c.requireAnalyticsRead(ctx); err != nil {
		return err
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil {
		return ctx.JSON(http.StatusOK, analyticsStatusResponse{})
	}
	stores := c.ext.parent.Stores()

	resp := analyticsStatusResponse{Enabled: stores.Analytics != nil}
	if tenants, terr := stores.AllTenants(ctx.Request().Context()); terr == nil {
		resp.TenantCount = len(tenants)
	}

	const lagThresholdSeconds = 60
	if stores.Analytics != nil {
		if lag, lerr := stores.Analytics.LagByTenant(ctx.Request().Context()); lerr == nil {
			resp.PerTenantLag = lag
			resp.WorstLagSeconds, resp.TenantsBehind = summarizeLag(lag, lagThresholdSeconds)
		}
	}

	return ctx.JSON(http.StatusOK, resp)
}
