package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/tenant"
)

// knownProjections are the projection planes fabriq keeps blue-green bookkeeping
// for (graph = FalkorDB, search = Elasticsearch). The relational source of truth
// is not a projection.
var knownProjections = []string{"graph", "search"}

// projectionStatusItem is the bookkeeping for one projection plane: its live
// pointer (status + target) and stream position.
type projectionStatusItem struct {
	Name string `json:"name"`
	// Status is the blue-green pointer state: live | building | soaking | abandoned.
	Status string `json:"status"`
	// ModelVersion is the _v{N} generation bumped by rebuilds.
	ModelVersion int `json:"modelVersion"`
	// EventVersion is the last applied event ULID (stream position); empty when
	// nothing has been applied through the projection engine.
	EventVersion string `json:"eventVersion"`
	// TargetName is the engine target currently receiving applies (e.g.
	// tenant_<id>_v2); empty when the default live target is used.
	TargetName string `json:"targetName"`
}

// projectionsResponse is the payload for GET {BasePath}/projections.
type projectionsResponse struct {
	Projections []projectionStatusItem `json:"projections"`
	// Backlog is the unpublished outbox depth — a proxy for projection lag (how
	// many committed events have not yet been forwarded to the change feed).
	Backlog int64 `json:"backlog"`
}

// registerProjectionRoutes wires the read-only projection-status route.
//
// This surface is intentionally read-only: rebuild and reconcile are heavy,
// worker-plane operations (blue-green target swaps / cross-tenant repair) driven
// by the fabriq-worker and the `fabriq reconcile`/`rebuild` CLIs, not by an
// interactive admin request.
func (c *adminController) registerProjectionRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.projections"),
		forge.WithSummary("Report projection bookkeeping (graph/search state + outbox backlog)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/projections", c.handleProjections, opts...); err != nil {
		return err
	}

	reconOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.projections.reconcile"),
		forge.WithSummary("Reconcile a projection against the source of truth (body: {projection, repair?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/projections/reconcile", c.handleProjectionReconcile, reconOpts...); err != nil {
		return err
	}

	rebuildOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.projections.rebuild"),
		forge.WithSummary("Rebuild a projection from the source of truth, then swap (body: {projection})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/projections/rebuild", c.handleProjectionRebuild, rebuildOpts...)
}

// projectionActionRequest is the body for the reconcile/rebuild endpoints.
type projectionActionRequest struct {
	// Projection is "graph" or "search".
	Projection string `json:"projection"`
	// Repair, on reconcile, republishes drifted aggregates through the pipeline
	// (reconcile only). Ignored by rebuild.
	Repair bool `json:"repair"`
}

// driftItem is one aggregate whose projected version differs from the truth.
type driftItem struct {
	Entity           string `json:"entity"`
	AggID            string `json:"aggId"`
	TruthVersion     int64  `json:"truthVersion"`
	ProjectedVersion int64  `json:"projectedVersion"`
}

// reconcileResponse is the payload for POST {BasePath}/projections/reconcile.
type reconcileResponse struct {
	Projection string      `json:"projection"`
	Repaired   bool        `json:"repaired"`
	DriftCount int         `json:"driftCount"`
	Drifts     []driftItem `json:"drifts"`
}

// rebuildResponse is the payload for POST {BasePath}/projections/rebuild.
type rebuildResponse struct {
	Projection string `json:"projection"`
	OldTarget  string `json:"oldTarget"`
	NewTarget  string `json:"newTarget"`
}

// reconcilerFor resolves the graph/search Reconciler from the opened adapters.
func (c *adminController) reconcilerFor(projName string) (*projection.Reconciler, error) {
	stores := c.ext.resolveStores()
	if stores == nil {
		return nil, errNoStores
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return nil, err
	}
	switch projName {
	case "graph":
		return stores.GraphReconciler(reg)
	case "search":
		return stores.SearchReconciler(reg)
	default:
		return nil, fmt.Errorf("unknown projection %q (want graph|search)", projName)
	}
}

// rebuilderFor resolves the graph/search Rebuilder from the opened adapters.
func (c *adminController) rebuilderFor(projName string) (*projection.Rebuilder, error) {
	stores := c.ext.resolveStores()
	if stores == nil {
		return nil, errNoStores
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return nil, err
	}
	switch projName {
	case "graph":
		return stores.GraphRebuilder(reg)
	case "search":
		return stores.SearchRebuilder(reg)
	default:
		return nil, fmt.Errorf("unknown projection %q (want graph|search)", projName)
	}
}

var errNoStores = fmt.Errorf("projection admin not available (no opened stores)")

// handleProjectionReconcile serves POST {BasePath}/projections/reconcile — scans
// the projection against the Postgres source of truth for the request tenant and
// reports drift (optionally repairing it through the pipeline when repair=true).
func (c *adminController) handleProjectionReconcile(ctx forge.Context) error {
	var req projectionActionRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	rec, err := c.reconcilerFor(req.Projection)
	if err != nil {
		return c.projectionAdminError(ctx, err)
	}

	reqCtx := ctx.Request().Context()
	tid, terr := tenant.FromContext(reqCtx)
	if terr != nil {
		return forge.BadRequest("tenant is required")
	}

	drifts, rErr := rec.Reconcile(reqCtx, tid, req.Repair)
	if rErr != nil {
		return renderError(ctx, rErr)
	}

	items := make([]driftItem, 0, len(drifts))
	for _, d := range drifts {
		items = append(items, driftItem{
			Entity:           d.Entity,
			AggID:            d.AggID,
			TruthVersion:     d.TruthVersion,
			ProjectedVersion: d.ProjectedVersion,
		})
	}
	return ctx.JSON(http.StatusOK, reconcileResponse{
		Projection: req.Projection,
		Repaired:   req.Repair,
		DriftCount: len(items),
		Drifts:     items,
	})
}

// handleProjectionRebuild serves POST {BasePath}/projections/rebuild — rebuilds
// the projection into a fresh target from the source of truth for the request
// tenant, then finalizes the blue-green swap (promotes the new target, abandons
// the old).
func (c *adminController) handleProjectionRebuild(ctx forge.Context) error {
	var req projectionActionRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	reb, err := c.rebuilderFor(req.Projection)
	if err != nil {
		return c.projectionAdminError(ctx, err)
	}

	reqCtx := ctx.Request().Context()
	tid, terr := tenant.FromContext(reqCtx)
	if terr != nil {
		return forge.BadRequest("tenant is required")
	}

	oldTarget, newTarget, rErr := reb.Rebuild(reqCtx, tid)
	if rErr != nil {
		return renderError(ctx, rErr)
	}
	// Promote the freshly built target and abandon the old one.
	if fErr := reb.Finalize(reqCtx, tid, oldTarget); fErr != nil {
		return renderError(ctx, fErr)
	}
	return ctx.JSON(http.StatusOK, rebuildResponse{
		Projection: req.Projection,
		OldTarget:  oldTarget,
		NewTarget:  newTarget,
	})
}

// projectionAdminError maps reconcile/rebuild resolution errors: no stores → 501,
// an unknown/unconfigured projection → 400.
func (c *adminController) projectionAdminError(ctx forge.Context, err error) error {
	if errors.Is(err, errNoStores) {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": err.Error()})
	}
	return forge.BadRequest(err.Error())
}

// handleProjections serves GET {BasePath}/projections — the graph/search
// projection state for the request tenant plus the outbox backlog. Returns 501
// when the instance has no Postgres-backed projection bookkeeping (e.g. a
// fake-backed test harness).
func (c *adminController) handleProjections(ctx forge.Context) error {
	sr := c.ext.resolveStateRepo()
	if sr == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{
			"error": "projection bookkeeping not available (no Postgres store)",
		})
	}

	reqCtx := ctx.Request().Context()
	tid, terr := tenant.FromContext(reqCtx)
	if terr != nil {
		return forge.BadRequest("tenant is required")
	}

	items := make([]projectionStatusItem, 0, len(knownProjections))
	for _, name := range knownProjections {
		// StateRepo.Get synthesises a {status:"live", modelVersion:1} default when
		// no explicit bookkeeping row exists, so an instance that has never run a
		// rebuild reports a clean "live" baseline rather than an error.
		st, err := sr.Get(reqCtx, tid, name)
		if err != nil {
			return renderError(ctx, err)
		}
		items = append(items, projectionStatusItem{
			Name:         name,
			Status:       st.Status,
			ModelVersion: st.ModelVersion,
			EventVersion: st.EventVersion,
			TargetName:   st.TargetName,
		})
	}

	backlog, err := c.unpublishedOutboxCount(reqCtx)
	if err != nil {
		return renderError(ctx, err)
	}

	return ctx.JSON(http.StatusOK, projectionsResponse{Projections: items, Backlog: backlog})
}

// unpublishedOutboxCount returns the tenant's unpublished outbox depth (the
// relay backlog). Shared by the events-backlog and projections endpoints. The
// outbox has no RLS, so the count is scoped to the tenant via the app.tenant_id
// GUC the relational tenant-tx stamps.
func (c *adminController) unpublishedOutboxCount(reqCtx context.Context) (int64, error) {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return 0, err
	}
	var rows []struct {
		N int64 `grove:"n"`
	}
	sql := `SELECT count(*) AS n FROM fabriq_outbox
		WHERE tenant_id = current_setting('app.tenant_id', true)
		  AND published_at IS NULL`
	if qErr := fab.Relational().Query(reqCtx, &rows, sql); qErr != nil {
		return 0, qErr
	}
	if len(rows) > 0 {
		return rows[0].N, nil
	}
	return 0, nil
}
