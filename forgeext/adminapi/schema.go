package adminapi

import (
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/registry"
)

// entityTypesResponse is the payload for GET {BasePath}/entities/types.
// It lists the registered DYNAMIC entity type names available to the admin
// (those backed by a registry.DynamicSchema); the read/write path supports
// dynamic entities only.
type entityTypesResponse struct {
	Types []string `json:"types"`
}

// schemaField is one field descriptor in a dynamic entity's schema: the column
// name and a simple, UI-oriented kind. It lets the admin SPA auto-generate a
// form for the entity.
type schemaField struct {
	// Name is the column name.
	Name string `json:"name"`
	// Kind is the simplified field kind: string, number, boolean, time, or object.
	Kind string `json:"kind"`
	// Required reports whether the column is NOT NULL.
	Required bool `json:"required"`
}

// schemaResponse is the payload for GET {BasePath}/schema. It carries the
// dynamic entity's field descriptors so the UI can build a form.
type schemaResponse struct {
	Type   string        `json:"type"`
	Fields []schemaField `json:"fields"`
}

// registerSchemaRoutes wires the type and schema introspection routes onto the
// given router. They share the same route options (auth middleware) as the
// entity-read routes.
func (c *adminController) registerSchemaRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	typesOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.types"),
		forge.WithSummary("List registered dynamic entity type names"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	// Register the static /entities/types route BEFORE the dynamic /entities/:id
	// detail route is matched: "types" must not be captured as an :id. Forge
	// matches static segments ahead of params, but registering it here keeps the
	// intent explicit.
	if err := r.GET(base+"/entities/types", c.handleEntityTypes, typesOpts...); err != nil {
		return err
	}

	schemaOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.schema"),
		forge.WithSummary("Get a dynamic entity's field descriptors (requires ?type=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/schema", c.handleSchema, schemaOpts...)
}

// handleEntityTypes serves GET {BasePath}/entities/types.
// It returns the sorted list of registered dynamic entity type names.
func (c *adminController) handleEntityTypes(ctx forge.Context) error {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	types := make([]string, 0)
	for _, ent := range reg.All() {
		if ent.Spec.Schema != nil {
			types = append(types, ent.Spec.Name)
		}
	}

	return ctx.JSON(http.StatusOK, entityTypesResponse{Types: types})
}

// handleSchema serves GET {BasePath}/schema.
//
// Required query params:
//
//	type  entity type name (e.g. "product")
//
// It returns the dynamic schema field descriptors for the type. An unknown type
// (or a non-dynamic / model-backed entity) yields 400.
func (c *adminController) handleSchema(ctx forge.Context) error {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	entityType := ctx.Query("type")
	if entityType == "" {
		return forge.BadRequest("query param 'type' is required")
	}

	ent, ok := reg.Get(entityType)
	if !ok || ent.Spec.Schema == nil {
		return forge.BadRequest("unknown dynamic entity type: " + entityType)
	}

	fields := make([]schemaField, 0, len(ent.Spec.Schema.Columns))
	for _, col := range ent.Spec.Schema.Columns {
		fields = append(fields, schemaField{
			Name:     col.Name,
			Kind:     columnKind(col.Type),
			Required: col.NotNull,
		})
	}

	return ctx.JSON(http.StatusOK, schemaResponse{Type: entityType, Fields: fields})
}

// columnKind maps a registry.ColumnType to a simple, UI-oriented kind string
// the admin SPA uses to choose a form control.
func columnKind(t registry.ColumnType) string {
	switch t {
	case registry.ColText:
		return "string"
	case registry.ColInt, registry.ColFloat:
		return "number"
	case registry.ColBool:
		return "boolean"
	case registry.ColTime:
		return "time"
	case registry.ColJSON:
		return "object"
	default:
		return "string"
	}
}
