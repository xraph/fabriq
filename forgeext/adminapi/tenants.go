package adminapi

import (
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/tenant"
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
	if err := r.POST(base+"/tenants/:id/resume", c.handleTenantResume, resumeOpts...); err != nil {
		return err
	}

	return nil
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
	switch fabriqerr.CodeOf(err) {
	case fabriqerr.CodeNotFound:
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
