package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

type server struct {
	fabric *fabriq.Fabriq
	auth   *authenticator
}

func (s *server) routes(r forge.Router) {
	api := r.Group("/api/v1")
	_ = api.POST("/sites", s.createSite)
	_ = api.GET("/sites/:id", s.getSite)
	_ = api.POST("/assets", s.createAsset)
	_ = api.PUT("/assets/:id", s.updateAsset)
	_ = api.GET("/assets/:id", s.getAsset)
	_ = api.GET("/assets", s.listAssets)
	// SSE uses a raw stdlib handler: the bridge needs the Flusher.
	_ = api.GET("/subscribe", s.subscribe)
}

// tenantCtx authenticates the request and returns a tenant-stamped
// context; on failure it writes the 401 itself and returns an error the
// caller uses only to stop.
func (s *server) tenantCtx(ctx forge.Context) (context.Context, error) {
	c, err := s.auth.Authenticate(ctx.Request())
	if err != nil {
		_ = ctx.JSON(http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return nil, err
	}
	return c, nil
}

type siteRequest struct {
	Name   string `json:"name"`
	Code   string `json:"code"`
	Region string `json:"region"`
}

func (s *server) createSite(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil // response already written
	}
	var req siteRequest
	if bindErr := ctx.Bind(&req); bindErr != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": bindErr.Error()})
	}
	res, err := s.fabric.Exec(tctx, command.Command{
		Entity: "site", Op: command.OpCreate,
		Payload: &domain.Site{Name: req.Name, Code: req.Code, Region: req.Region},
	})
	if err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusCreated, res)
}

func (s *server) getSite(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var site domain.Site
	if err := s.fabric.Relational().Get(tctx, "site", ctx.Param("id"), &site); err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, site)
}

type assetRequest struct {
	Name            string `json:"name"`
	Kind            string `json:"kind"`
	Serial          string `json:"serial"`
	SiteID          string `json:"site_id"`
	ParentID        string `json:"parent_id"`
	ExpectedVersion *int64 `json:"expected_version"`
}

func (s *server) createAsset(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var req assetRequest
	if bindErr := ctx.Bind(&req); bindErr != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": bindErr.Error()})
	}
	res, err := s.fabric.Exec(tctx, command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: req.Name, Kind: req.Kind, Serial: req.Serial, SiteID: req.SiteID, ParentID: req.ParentID},
	})
	if err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusCreated, res)
}

func (s *server) updateAsset(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var req assetRequest
	if bindErr := ctx.Bind(&req); bindErr != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": bindErr.Error()})
	}
	res, err := s.fabric.Exec(tctx, command.Command{
		Entity: "asset", Op: command.OpUpdate, AggID: ctx.Param("id"),
		Payload:         &domain.Asset{Name: req.Name, Kind: req.Kind, Serial: req.Serial, SiteID: req.SiteID, ParentID: req.ParentID},
		ExpectedVersion: req.ExpectedVersion,
	})
	if err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, res)
}

func (s *server) getAsset(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	var asset domain.Asset
	if err := s.fabric.Relational().Get(tctx, "asset", ctx.Param("id"), &asset); err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, asset)
}

func (s *server) listAssets(ctx forge.Context) error {
	tctx, err := s.tenantCtx(ctx)
	if err != nil {
		return nil
	}
	q := query.ListQuery{OrderBy: "name", Limit: 100}
	if siteID := ctx.Query("site_id"); siteID != "" {
		q.Filter = map[string]any{"site_id": siteID}
	}
	var assets []*domain.Asset
	if err := s.fabric.Relational().List(tctx, "asset", q, &assets); err != nil {
		return writeCommandError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, assets)
}

func writeCommandError(ctx forge.Context, err error) error {
	switch {
	case errors.Is(err, fabriq.ErrNotFound):
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, fabriq.ErrVersionConflict):
		return ctx.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, fabriq.ErrNoTenant):
		return ctx.JSON(http.StatusUnauthorized, map[string]string{"error": err.Error()})
	default:
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
}
