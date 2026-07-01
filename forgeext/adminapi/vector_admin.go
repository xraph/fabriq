package adminapi

import (
	"encoding/json"
	"math"
	"net/http"

	"github.com/xraph/forge"
)

// vectorPreviewLen caps how many leading components of an embedding the inspect
// endpoint echoes — enough to eyeball the vector without shipping all 768 floats.
const vectorPreviewLen = 12

// vectorGetResponse is the payload for GET {BasePath}/vector/:entity/:id — a
// read-only inspection of one stored embedding.
type vectorGetResponse struct {
	Entity  string    `json:"entity"`
	ID      string    `json:"id"`
	Dims    int       `json:"dims"`
	Norm    float64   `json:"norm"`
	Preview []float32 `json:"preview"`
}

// vectorDeleteResponse is the payload for the embedding delete endpoints.
type vectorDeleteResponse struct {
	Deleted bool `json:"deleted"`
}

// vectorDeleteByMetaRequest is the body for POST {BasePath}/vector/delete-by-meta.
type vectorDeleteByMetaRequest struct {
	// Entity is the registered entity whose embeddings are targeted.
	Entity string `json:"entity"`
	// Filter is an AND-of-equals over embedding meta. An EMPTY filter would
	// delete every embedding for the entity, so it is rejected unless All is set.
	Filter map[string]string `json:"filter"`
	// All must be explicitly true to opt into the wipe-all (empty-filter) path.
	All bool `json:"all"`
}

// registerVectorAdminRoutes wires the embedding inspect/delete routes. They
// share the admin surface's route options (auth/tenant middleware).
func (c *adminController) registerVectorAdminRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	getOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.vector.get"),
		forge.WithSummary("Inspect a stored embedding (dims, L2 norm, preview)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/vector/:entity/:id", c.handleVectorGet, getOpts...); err != nil {
		return err
	}

	delOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.vector.delete"),
		forge.WithSummary("Delete one stored embedding"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.DELETE(base+"/vector/:entity/:id", c.handleVectorDelete, delOpts...); err != nil {
		return err
	}

	dbmOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.vector.deleteByMeta"),
		forge.WithSummary("Delete embeddings by meta filter (body: {entity, filter, all?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/vector/delete-by-meta", c.handleVectorDeleteByMeta, dbmOpts...)
}

// handleVectorGet serves GET {BasePath}/vector/:entity/:id.
//
// Returns the embedding's dimensionality, L2 norm and a leading preview. 404
// when no embedding is stored for (entity, id); 501 when vector is unconfigured.
func (c *adminController) handleVectorGet(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	entity := ctx.Param("entity")
	id := ctx.Param("id")
	if entity == "" || id == "" {
		return forge.BadRequest("path params 'entity' and 'id' are required")
	}

	reqCtx := ctx.Request().Context()
	vec := fab.Vector()
	if !vectorConfigured(reqCtx, vec) {
		return c.vectorNotConfigured(ctx)
	}

	emb, getErr := vec.Get(reqCtx, entity, id)
	if getErr != nil {
		return mapQueryError(getErr)
	}

	var sumSq float64
	for _, v := range emb {
		sumSq += float64(v) * float64(v)
	}
	preview := emb
	if len(preview) > vectorPreviewLen {
		preview = preview[:vectorPreviewLen]
	}
	return ctx.JSON(http.StatusOK, vectorGetResponse{
		Entity:  entity,
		ID:      id,
		Dims:    len(emb),
		Norm:    math.Sqrt(sumSq),
		Preview: preview,
	})
}

// handleVectorDelete serves DELETE {BasePath}/vector/:entity/:id. Deleting a
// missing embedding is a no-op (idempotent), so it always reports deleted=true.
func (c *adminController) handleVectorDelete(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	entity := ctx.Param("entity")
	id := ctx.Param("id")
	if entity == "" || id == "" {
		return forge.BadRequest("path params 'entity' and 'id' are required")
	}

	reqCtx := ctx.Request().Context()
	vec := fab.Vector()
	if !vectorConfigured(reqCtx, vec) {
		return c.vectorNotConfigured(ctx)
	}

	if delErr := vec.Delete(reqCtx, entity, id); delErr != nil {
		return mapQueryError(delErr)
	}
	return ctx.JSON(http.StatusOK, vectorDeleteResponse{Deleted: true})
}

// handleVectorDeleteByMeta serves POST {BasePath}/vector/delete-by-meta.
//
// Removes every embedding for the entity whose meta contains all key/value pairs
// in filter (AND-of-equals). An empty filter is the wipe-all path and is rejected
// unless {all:true} is explicitly set. 501 when vector is unconfigured.
func (c *adminController) handleVectorDeleteByMeta(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req vectorDeleteByMetaRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Entity == "" {
		return forge.BadRequest("field 'entity' is required")
	}
	if len(req.Filter) == 0 && !req.All {
		return forge.BadRequest("provide a non-empty 'filter', or set 'all': true to delete ALL embeddings for the entity")
	}

	reqCtx := ctx.Request().Context()
	vec := fab.Vector()
	if !vectorConfigured(reqCtx, vec) {
		return c.vectorNotConfigured(ctx)
	}

	filter := req.Filter
	if req.All {
		filter = map[string]string{} // explicit wipe-all
	}
	if delErr := vec.DeleteByMeta(reqCtx, req.Entity, filter); delErr != nil {
		return mapQueryError(delErr)
	}
	return ctx.JSON(http.StatusOK, vectorDeleteResponse{Deleted: true})
}
