package client

import (
	"context"
	"net/http"
)

// ProjectionStatus is the bookkeeping for one projection plane (graph or
// search): its blue-green pointer and stream position. It mirrors
// adminapi's projectionStatusItem JSON exactly:
// {name, status, modelVersion, eventVersion, targetName}.
type ProjectionStatus struct {
	Name string `json:"name"`
	// Status is the blue-green pointer state: live | building | soaking | abandoned.
	Status string `json:"status"`
	// ModelVersion is the _v{N} generation bumped by rebuilds.
	ModelVersion int `json:"modelVersion"`
	// EventVersion is the last applied event ULID (stream position); empty
	// when nothing has been applied through the projection engine.
	EventVersion string `json:"eventVersion"`
	// TargetName is the engine target currently receiving applies (e.g.
	// tenant_<id>_v2); empty when the default live target is used.
	TargetName string `json:"targetName"`
}

// ProjectionsInfo is the payload for GetProjections. It mirrors adminapi's
// projectionsResponse JSON exactly: {projections, backlog}.
type ProjectionsInfo struct {
	Projections []ProjectionStatus `json:"projections"`
	// Backlog is the unpublished outbox depth — a proxy for projection lag.
	Backlog int64 `json:"backlog"`
}

// ProjectionDrift is one aggregate whose projected version differs from the
// source of truth. It mirrors adminapi's driftItem JSON exactly:
// {entity, aggId, truthVersion, projectedVersion}.
type ProjectionDrift struct {
	Entity           string `json:"entity"`
	AggID            string `json:"aggId"`
	TruthVersion     int64  `json:"truthVersion"`
	ProjectedVersion int64  `json:"projectedVersion"`
}

// ProjectionReconcileResult is the payload for ProjectionReconcile. It
// mirrors adminapi's reconcileResponse JSON exactly:
// {projection, repaired, driftCount, drifts}.
type ProjectionReconcileResult struct {
	Projection string            `json:"projection"`
	Repaired   bool              `json:"repaired"`
	DriftCount int               `json:"driftCount"`
	Drifts     []ProjectionDrift `json:"drifts"`
}

// ProjectionRebuildResult is the payload for ProjectionRebuild. It mirrors
// adminapi's rebuildResponse JSON exactly:
// {projection, oldTarget, newTarget}.
type ProjectionRebuildResult struct {
	Projection string `json:"projection"`
	OldTarget  string `json:"oldTarget"`
	NewTarget  string `json:"newTarget"`
}

// GetProjections reports the graph/search projection bookkeeping for the
// active tenant plus the outbox backlog (a lag proxy). It calls
// GET {BasePath}/projections. Returns an *APIError with Status 501 when the
// instance has no Postgres-backed projection bookkeeping.
func (c *Client) GetProjections(ctx context.Context) (ProjectionsInfo, error) {
	var out ProjectionsInfo
	if err := c.do(ctx, http.MethodGet, "/projections", nil, nil, &out); err != nil {
		return ProjectionsInfo{}, err
	}
	return out, nil
}

// ProjectionReconcile scans a projection ("graph" or "search") against the
// Postgres source of truth for the active tenant and reports drift,
// optionally repairing it (republishing drifted aggregates through the
// pipeline) when repair is true. It calls
// POST {BasePath}/projections/reconcile with body {projection, repair}.
func (c *Client) ProjectionReconcile(ctx context.Context, projection string, repair bool) (ProjectionReconcileResult, error) {
	var out ProjectionReconcileResult
	body := map[string]any{"projection": projection, "repair": repair}
	if err := c.do(ctx, http.MethodPost, "/projections/reconcile", nil, body, &out); err != nil {
		return ProjectionReconcileResult{}, err
	}
	return out, nil
}

// ProjectionRebuild rebuilds a projection ("graph" or "search") from the
// source of truth into a fresh target for the active tenant, then finalizes
// the blue-green swap (promotes the new target, abandons the old). It calls
// POST {BasePath}/projections/rebuild with body {projection}.
func (c *Client) ProjectionRebuild(ctx context.Context, projection string) (ProjectionRebuildResult, error) {
	var out ProjectionRebuildResult
	body := map[string]any{"projection": projection}
	if err := c.do(ctx, http.MethodPost, "/projections/rebuild", nil, body, &out); err != nil {
		return ProjectionRebuildResult{}, err
	}
	return out, nil
}
