package adminapi

import (
	"encoding/json"
	"net/http"

	"github.com/xraph/forge"
)

// issueKeyRequest is the request body for POST {BasePath}/keys.
type issueKeyRequest struct {
	Label         string `json:"label"`
	TenantID      string `json:"tenantId,omitempty"`
	CanManageKeys bool   `json:"canManageKeys,omitempty"`
}

// issueKeyResponse is the payload for POST {BasePath}/keys. Key is the
// plaintext bearer token, returned once at issue time — it is never persisted
// or recoverable afterwards.
type issueKeyResponse struct {
	ID     string `json:"id"`
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
}

// keyItem is a single stored key as seen on the read paths. It deliberately
// carries no key/hash field: redaction is structural, mirroring KeyRecord.
type keyItem struct {
	ID            string  `json:"id"`
	Prefix        string  `json:"prefix"`
	Label         string  `json:"label"`
	TenantID      string  `json:"tenantId,omitempty"`
	CanManageKeys bool    `json:"canManageKeys"`
	CreatedAt     string  `json:"createdAt"`
	RevokedAt     *string `json:"revokedAt,omitempty"`
}

// keyListResponse is the payload for GET {BasePath}/keys.
type keyListResponse struct {
	Keys []keyItem `json:"keys"`
}

// revokeKeyResponse is the payload for DELETE {BasePath}/keys/:id.
type revokeKeyResponse struct {
	Revoked bool `json:"revoked"`
}

// registerKeyRoutes wires the API-key management routes (issue/list/revoke)
// onto the given router. It is called from adminController.Routes ONLY when
// the host enabled auth via WithAuth (cfg.KeyStore != nil): without a store
// there is nothing to manage. No middleware is installed here — the host
// attaches authMiddleware via WithRouteOptions (see authMiddleware's
// CanManageKeys gate on the /keys sub-path).
func (c *adminController) registerKeyRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	issueOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.keys.issue"),
		forge.WithSummary("Issue a new API key (body: {label, tenantId?, canManageKeys?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/keys", c.handleIssueKey, issueOpts...); err != nil {
		return err
	}

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.keys.list"),
		forge.WithSummary("List API keys (redacted — no key/hash field)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/keys", c.handleListKeys, listOpts...); err != nil {
		return err
	}

	revokeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.keys.revoke"),
		forge.WithSummary("Revoke an API key by id"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.DELETE(base+"/keys/:id", c.handleRevokeKey, revokeOpts...)
}

// handleIssueKey serves POST {BasePath}/keys.
//
// Request body:
//
//	{ "label": "<name>", "tenantId": "<id>"?, "canManageKeys": bool? }
//
// tenantId is optional: omitted/empty issues a multi-tenant key (callable
// against any tenant via the X-Tenant-ID selector); a non-empty tenantId
// scopes the key to that single tenant. On success it returns 201 with
// {id, prefix, key} — key is the plaintext bearer token, returned once.
func (c *adminController) handleIssueKey(ctx forge.Context) error {
	store := c.ext.cfg.KeyStore
	if store == nil {
		return forge.InternalError(nil)
	}

	var req issueKeyRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Label == "" {
		return forge.BadRequest("field 'label' is required")
	}

	issued, err := store.Issue(ctx.Request().Context(), KeySpec{
		Label:         req.Label,
		TenantID:      req.TenantID,
		CanManageKeys: req.CanManageKeys,
	})
	if err != nil {
		return renderError(ctx, err)
	}

	return ctx.JSON(http.StatusCreated, issueKeyResponse{
		ID:     issued.ID,
		Prefix: issued.Prefix,
		Key:    issued.Key,
	})
}

// handleListKeys serves GET {BasePath}/keys.
//
// Returns 200 with {keys: [...]}, each entry redacted (no key/hash field).
func (c *adminController) handleListKeys(ctx forge.Context) error {
	store := c.ext.cfg.KeyStore
	if store == nil {
		return forge.InternalError(nil)
	}

	recs, err := store.List(ctx.Request().Context())
	if err != nil {
		return renderError(ctx, err)
	}

	items := make([]keyItem, 0, len(recs))
	for _, rec := range recs {
		item := keyItem{
			ID:            rec.ID,
			Prefix:        rec.Prefix,
			Label:         rec.Label,
			TenantID:      rec.TenantID,
			CanManageKeys: rec.CanManageKeys,
			CreatedAt:     rec.CreatedAt.Format(rfc3339Milli),
		}
		if rec.RevokedAt != nil {
			s := rec.RevokedAt.Format(rfc3339Milli)
			item.RevokedAt = &s
		}
		items = append(items, item)
	}

	return ctx.JSON(http.StatusOK, keyListResponse{Keys: items})
}

// rfc3339Milli formats timestamps with millisecond precision for JSON responses.
const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

// handleRevokeKey serves DELETE {BasePath}/keys/:id.
//
// Returns 200 with {revoked: true} on success. A missing id yields 400.
func (c *adminController) handleRevokeKey(ctx forge.Context) error {
	store := c.ext.cfg.KeyStore
	if store == nil {
		return forge.InternalError(nil)
	}

	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	if err := store.Revoke(ctx.Request().Context(), id); err != nil {
		return renderError(ctx, err)
	}

	return ctx.JSON(http.StatusOK, revokeKeyResponse{Revoked: true})
}
