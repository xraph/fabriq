package adminapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
)

// --- wire types ---

type schemaWriteColumn struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	Default  string `json:"default,omitempty"`
}

type schemaWriteIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
}

type defineSchemaRequest struct {
	Type    string              `json:"type"`
	Columns []schemaWriteColumn `json:"columns"`
	Indexes []schemaWriteIndex  `json:"indexes,omitempty"`
}

type addFieldsRequest struct {
	Columns []schemaWriteColumn `json:"columns"`
	Indexes []schemaWriteIndex  `json:"indexes,omitempty"`
}

type renameFieldRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// --- helpers ---

var schemaIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validSchemaIdent(s string) bool { return schemaIdentRe.MatchString(s) }

func tableFor(typeName string) string { return "ds_" + typeName }

func kindToColumnType(kind string) (registry.ColumnType, error) {
	switch kind {
	case "string":
		return registry.ColText, nil
	case "number":
		return registry.ColFloat, nil
	case "boolean":
		return registry.ColBool, nil
	case "time":
		return registry.ColTime, nil
	case "object":
		return registry.ColJSON, nil
	default:
		return 0, fmt.Errorf("unknown column kind %q (want string|number|boolean|time|object)", kind)
	}
}

var (
	defNumRe = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
	defStrRe = regexp.MustCompile(`^'[^']*'$`)
)

// validateDefaultExpr allows only a strict allowlist of SQL default expressions,
// because DynamicColumn.Default is interpolated verbatim into DDL.
func validateDefaultExpr(s string) error {
	if s == "" {
		return nil
	}
	switch strings.ToLower(s) {
	case "true", "false", "null", "now()":
		return nil
	}
	if defNumRe.MatchString(s) || defStrRe.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid column default %q: allowed forms are a number, true/false/null, now(), or a single-quoted string literal", s)
}

// columnsToRegistry translates wire columns to registry.DynamicColumn, validating
// each name/kind/default. Returns a 400-worthy error on the first problem.
func columnsToRegistry(cols []schemaWriteColumn) ([]registry.DynamicColumn, error) {
	out := make([]registry.DynamicColumn, 0, len(cols))
	for _, c := range cols {
		if !validSchemaIdent(c.Name) {
			return nil, fmt.Errorf("invalid column name %q", c.Name)
		}
		if registry.IsReservedColumn(c.Name) {
			return nil, fmt.Errorf("column %q is a reserved structural name (id, tenant_id and version are managed by fabriq)", c.Name)
		}
		ct, err := kindToColumnType(c.Kind)
		if err != nil {
			return nil, err
		}
		if err := validateDefaultExpr(c.Default); err != nil {
			return nil, err
		}
		out = append(out, registry.DynamicColumn{Name: c.Name, Type: ct, NotNull: c.Required, Default: c.Default})
	}
	return out, nil
}

func indexesToRegistry(idx []schemaWriteIndex) ([]registry.DynamicIndex, error) {
	out := make([]registry.DynamicIndex, 0, len(idx))
	for _, i := range idx {
		if !validSchemaIdent(i.Name) {
			return nil, fmt.Errorf("invalid index name %q", i.Name)
		}
		for _, c := range i.Columns {
			if !validSchemaIdent(c) {
				return nil, fmt.Errorf("invalid index column %q", c)
			}
		}
		out = append(out, registry.DynamicIndex{Name: i.Name, Columns: i.Columns, Unique: i.Unique})
	}
	return out, nil
}

// --- routes ---

// registerSchemaWriteRoutes wires the dynamic-schema write routes (define,
// add-fields, rename-field, drop-field, drop-type) onto the given router. They
// share the same route options (auth middleware) as the other admin routes.
func (c *adminController) registerSchemaWriteRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := c.ext.cfg.RouteOptions
	with := func(name, summary string) []forge.RouteOption {
		return append([]forge.RouteOption{
			forge.WithName(name), forge.WithSummary(summary), forge.WithTags("Fabriq", "Admin"),
		}, opts...)
	}
	if err := r.POST(base+"/schema", c.handleDefineDynamic, with("fabriq.admin.schema.define", "Define a dynamic entity type")...); err != nil {
		return err
	}
	if err := r.POST(base+"/schema/:type/fields", c.handleAddFields, with("fabriq.admin.schema.addFields", "Add fields to a dynamic type")...); err != nil {
		return err
	}
	if err := r.POST(base+"/schema/:type/rename-field", c.handleRenameField, with("fabriq.admin.schema.renameField", "Rename a field of a dynamic type")...); err != nil {
		return err
	}
	if err := r.DELETE(base+"/schema/:type/fields/:column", c.handleDropField, with("fabriq.admin.schema.dropField", "Drop a field (requires ?confirm=<column>)")...); err != nil {
		return err
	}
	return r.DELETE(base+"/schema/:type", c.handleDropType, with("fabriq.admin.schema.drop", "Drop a dynamic type (requires ?confirm=<type>)")...)
}

// handleDefineDynamic serves POST {BasePath}/schema.
//
// Request body: { "type": "<name>", "columns": [...], "indexes": [...] }
//
// Defines a new dynamic entity type and creates its backing table. A bad kind
// or default expression yields 400; a Fabriq assembled without Postgres yields
// 501.
func (c *adminController) handleDefineDynamic(ctx forge.Context) error {
	w, err := c.ext.resolveDynamicWriter()
	if err != nil {
		return forge.InternalError(err)
	}
	var req defineSchemaRequest
	if e := json.NewDecoder(ctx.Request().Body).Decode(&req); e != nil {
		return forge.BadRequest("invalid request body: " + e.Error())
	}
	if !validSchemaIdent(req.Type) {
		return forge.BadRequest("invalid type name: " + req.Type)
	}
	cols, e := columnsToRegistry(req.Columns)
	if e != nil {
		return forge.BadRequest(e.Error())
	}
	idx, e := indexesToRegistry(req.Indexes)
	if e != nil {
		return forge.BadRequest(e.Error())
	}
	spec := registry.EntitySpec{
		Name: req.Type, Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{Table: tableFor(req.Type), Columns: cols, Indexes: idx},
	}
	if err := w.DefineDynamic(ctx.Request().Context(), spec); err != nil {
		return mapSchemaError(ctx, err)
	}
	return ctx.JSON(http.StatusCreated, schemaResponse{Type: req.Type, Fields: specFields(spec)})
}

// handleAddFields serves POST {BasePath}/schema/:type/fields.
//
// Request body: { "columns": [...], "indexes": [...] }
//
// The type must already be a registered dynamic entity (404 otherwise). The
// facade's AlterDynamic re-registers the ENTIRE spec it is given, so the
// handler fetches the current schema and unions its existing columns with the
// new ones (new column names win on conflict) before calling AlterDynamic —
// passing only the new columns would drop the existing ones from the registry
// descriptor.
func (c *adminController) handleAddFields(ctx forge.Context) error {
	w, err := c.ext.resolveDynamicWriter()
	if err != nil {
		return forge.InternalError(err)
	}
	typeName := ctx.Param("type")
	if !validSchemaIdent(typeName) {
		return forge.BadRequest("invalid type name: " + typeName)
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}
	ent, ok := reg.Get(typeName)
	if !ok || ent.Spec.Schema == nil {
		return forge.NotFound("unknown dynamic entity type: " + typeName)
	}
	var req addFieldsRequest
	if e := json.NewDecoder(ctx.Request().Body).Decode(&req); e != nil {
		return forge.BadRequest("invalid request body: " + e.Error())
	}
	cols, e := columnsToRegistry(req.Columns)
	if e != nil {
		return forge.BadRequest(e.Error())
	}
	idx, e := indexesToRegistry(req.Indexes)
	if e != nil {
		return forge.BadRequest(e.Error())
	}
	spec := registry.EntitySpec{
		Name: typeName, Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table:   ent.Spec.Schema.Table,
			Columns: unionColumns(ent.Spec.Schema.Columns, cols),
			Indexes: idx,
		},
	}
	if err := w.AlterDynamic(ctx.Request().Context(), spec); err != nil {
		return mapSchemaError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, schemaResponse{Type: typeName, Fields: specFields(spec)})
}

// unionColumns merges existing with add, keeping existing's order and
// appending genuinely new columns at the end. Where a name appears in both,
// add's definition wins (it is the caller's latest intent for that column).
func unionColumns(existing, add []registry.DynamicColumn) []registry.DynamicColumn {
	byName := make(map[string]registry.DynamicColumn, len(add))
	for _, c := range add {
		byName[c.Name] = c
	}
	out := make([]registry.DynamicColumn, 0, len(existing)+len(add))
	seen := make(map[string]bool, len(existing))
	for _, c := range existing {
		if nc, ok := byName[c.Name]; ok {
			out = append(out, nc)
		} else {
			out = append(out, c)
		}
		seen[c.Name] = true
	}
	for _, c := range add {
		if !seen[c.Name] {
			out = append(out, c)
		}
	}
	return out
}

// handleRenameField serves POST {BasePath}/schema/:type/rename-field.
//
// Request body: { "from": "<oldColumn>", "to": "<newColumn>" }
func (c *adminController) handleRenameField(ctx forge.Context) error {
	w, err := c.ext.resolveDynamicWriter()
	if err != nil {
		return forge.InternalError(err)
	}
	typeName := ctx.Param("type")
	var req renameFieldRequest
	if e := json.NewDecoder(ctx.Request().Body).Decode(&req); e != nil {
		return forge.BadRequest("invalid request body: " + e.Error())
	}
	if !validSchemaIdent(typeName) || !validSchemaIdent(req.From) || !validSchemaIdent(req.To) {
		return forge.BadRequest("invalid identifier in rename")
	}
	if err := w.RenameDynamicField(ctx.Request().Context(), typeName, req.From, req.To); err != nil {
		return mapSchemaError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, map[string]string{"type": typeName, "from": req.From, "to": req.To})
}

// handleDropField serves DELETE {BasePath}/schema/:type/fields/:column.
//
// Required query params:
//
//	confirm  must equal the column name being dropped (guards accidental drops)
func (c *adminController) handleDropField(ctx forge.Context) error {
	w, err := c.ext.resolveDynamicWriter()
	if err != nil {
		return forge.InternalError(err)
	}
	typeName, col := ctx.Param("type"), ctx.Param("column")
	if !validSchemaIdent(typeName) || !validSchemaIdent(col) {
		return forge.BadRequest("invalid identifier")
	}
	if ctx.Query("confirm") != col {
		return forge.BadRequest("confirmation required: pass ?confirm=" + col)
	}
	if err := w.DropDynamicField(ctx.Request().Context(), typeName, col); err != nil {
		return mapSchemaError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, map[string]string{"type": typeName, "dropped": col})
}

// handleDropType serves DELETE {BasePath}/schema/:type.
//
// Required query params:
//
//	confirm  must equal the type name being dropped (guards accidental drops)
func (c *adminController) handleDropType(ctx forge.Context) error {
	w, err := c.ext.resolveDynamicWriter()
	if err != nil {
		return forge.InternalError(err)
	}
	typeName := ctx.Param("type")
	if !validSchemaIdent(typeName) {
		return forge.BadRequest("invalid type name: " + typeName)
	}
	if ctx.Query("confirm") != typeName {
		return forge.BadRequest("confirmation required: pass ?confirm=" + typeName)
	}
	if err := w.DropDynamic(ctx.Request().Context(), typeName); err != nil {
		return mapSchemaError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, map[string]string{"dropped": typeName})
}

// specFields renders the spec's columns as the read wire shape (reuse
// schemaField/columnKind from schema.go).
func specFields(spec registry.EntitySpec) []schemaField {
	out := make([]schemaField, 0, len(spec.Schema.Columns))
	for _, c := range spec.Schema.Columns {
		out = append(out, schemaField{Name: c.Name, Kind: columnKind(c.Type), Required: c.NotNull})
	}
	return out
}

// mapSchemaError maps facade errors to HTTP: unavailable (no Postgres) → 501,
// duplicate define (type already exists) → 409, not-found (unknown/non-dynamic
// type) → 404, structural/validation guard errors → 400, else 500.
//
// The SP1 dynamic-lifecycle facade (fabriq_dynamic.go) returns plain
// fmt.Errorf-wrapped strings rather than sentinels for its not-found cases
// ("cannot alter unknown entity %q", "cannot drop unknown entity %q", "unknown
// dynamic entity %q"), so those are matched by substring. ErrDynamicUnavailable
// IS a sentinel and is matched with errors.Is.
func mapSchemaError(ctx forge.Context, err error) error {
	if err == nil {
		return nil
	}
	// Structured errors (e.g. classified DDL faults from the postgres adapter)
	// render directly: proper Code→HTTP status and NO driver text or SQL in the
	// response body.
	var fe *fabriqerr.Error
	if errors.As(err, &fe) {
		return renderError(ctx, err)
	}
	if errors.Is(err, fabriq.ErrDynamicUnavailable) {
		return forge.NewHTTPError(http.StatusNotImplemented, err.Error())
	}
	msg := err.Error()
	if strings.Contains(msg, "registered twice") {
		return forge.NewHTTPError(http.StatusConflict, msg)
	}
	if strings.Contains(msg, "unknown entity") ||
		strings.Contains(msg, "unknown dynamic entity") ||
		strings.Contains(msg, "cannot drop unknown") ||
		strings.Contains(msg, "cannot alter unknown") ||
		strings.Contains(msg, "cannot replace unknown") {
		return forge.NotFound(msg)
	}
	if strings.Contains(msg, "not dynamic") || strings.Contains(msg, "Schema is nil") ||
		strings.Contains(msg, "must be KindAggregate") || strings.Contains(msg, "invalid") {
		return forge.BadRequest(msg)
	}
	// Anything else is rendered generically (safe message, no leak) rather than
	// dumping err.Error() into the response.
	return renderError(ctx, err)
}
