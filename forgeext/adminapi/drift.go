package adminapi

import (
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/adapters/postgres"
)

// driftEntity reports one entity's registry-vs-physical schema drift.
type driftEntity struct {
	Entity  string   `json:"entity"`
	Table   string   `json:"table"`
	Dynamic bool     `json:"dynamic"`
	InSync  bool     `json:"inSync"`
	Missing []string `json:"missing"` // expected in the registry, absent physically
	Extra   []string `json:"extra"`   // present physically, not in the registry
	Error   string   `json:"error,omitempty"` // set when this entity's table could not be introspected
}

type driftResponse struct {
	Entities []driftEntity `json:"entities"`
}

// computeDrift compares the registry's expected column names against the
// physical columns. missing = expected but not present; extra = present but not
// expected. Order is registry order for missing, physical order for extra.
func computeDrift(expected []string, physical []postgres.ColumnInfo) (missing, extra []string) {
	phys := make(map[string]bool, len(physical))
	for _, c := range physical {
		phys[c.Name] = true
	}
	exp := make(map[string]bool, len(expected))
	for _, e := range expected {
		exp[e] = true
		if !phys[e] {
			missing = append(missing, e)
		}
	}
	for _, c := range physical {
		if !exp[c.Name] {
			extra = append(extra, c.Name)
		}
	}
	return missing, extra
}

// registerDriftRoutes wires the read-only GET {BasePath}/schema/drift route.
func (c *adminController) registerDriftRoutes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.schema.drift"),
		forge.WithSummary("Registry-vs-physical schema drift for every entity"),
		forge.WithTags("Fabriq", "Admin"),
	}, c.ext.cfg.RouteOptions...)
	return r.GET(c.ext.cfg.BasePath+"/schema/drift", c.handleSchemaDrift, opts...)
}

// handleSchemaDrift serves GET {BasePath}/schema/drift — read-only. Reports, per
// registered entity, the columns the registry expects that are missing from the
// physical table (migrations behind) and the physical columns not in the
// registry (hand-edited / stale).
func (c *adminController) handleSchemaDrift(ctx forge.Context) error {
	stores := c.ext.resolveStores()
	if stores == nil || stores.Postgres == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "drift not available (no relational store)"})
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return renderError(ctx, err)
	}
	reqCtx := ctx.Request().Context()
	out := driftResponse{Entities: make([]driftEntity, 0)}
	for _, ent := range reg.All() {
		table := ent.Binding.Table
		de := driftEntity{Entity: ent.Spec.Name, Table: table, Dynamic: ent.Binding.IsDynamic(), Missing: []string{}, Extra: []string{}}
		physical, terr := stores.Postgres.TableColumns(reqCtx, table)
		if terr != nil {
			// Diagnostics surface: one bad table must not blank the whole report.
			de.Error = terr.Error()
			de.InSync = false
			out.Entities = append(out.Entities, de)
			continue
		}
		missing, extra := computeDrift(ent.Binding.Columns, physical)
		// Preserve the non-nil defaults so the JSON contract is always [] (a nil
		// []string marshals to null, which breaks clients that call .join()).
		if missing != nil {
			de.Missing = missing
		}
		if extra != nil {
			de.Extra = extra
		}
		de.InSync = len(missing) == 0 && len(extra) == 0
		out.Entities = append(out.Entities, de)
	}
	return ctx.JSON(http.StatusOK, out)
}
