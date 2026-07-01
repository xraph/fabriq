package adminapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/tenant"
)

// adhocDDLRequest is the body for POST {BasePath}/schema/ddl.
type adhocDDLRequest struct {
	SQL string `json:"sql"`
}

// precheckSingleStatement rejects empty input and statement stacking (a single
// trailing semicolon is allowed). Crude by design — the operator opted into an
// escape hatch; this only prevents accidental multi-statement blasts.
func precheckSingleStatement(sql string) error {
	s := strings.TrimSpace(sql)
	if s == "" {
		return fmt.Errorf("empty statement")
	}
	if strings.Contains(strings.TrimSuffix(s, ";"), ";") {
		return fmt.Errorf("only a single statement is allowed")
	}
	return nil
}

// registerDDLRoutes wires POST {BasePath}/schema/ddl — the gated ad-hoc DDL
// escape hatch (deliberately OUTSIDE the migration authority).
func (c *adminController) registerDDLRoutes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.schema.ddl"),
		forge.WithSummary("Run a single ad-hoc DDL statement (gated; not a migration)"),
		forge.WithTags("Fabriq", "Admin"),
	}, c.ext.cfg.RouteOptions...)
	return r.POST(c.ext.cfg.BasePath+"/schema/ddl", c.handleAdhocDDL, opts...)
}

// handleAdhocDDL serves POST {BasePath}/schema/ddl — gated escape hatch. Runs a
// single raw DDL statement as the schema owner. It is NOT recorded in the
// migration ledger; every attempt is audited to the structured log.
func (c *adminController) handleAdhocDDL(ctx forge.Context) error {
	if err := c.requireSchemaAdmin(ctx); err != nil {
		return err
	}
	var req adhocDDLRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid request body: " + err.Error())
	}
	if err := precheckSingleStatement(req.SQL); err != nil {
		return forge.BadRequest(err.Error())
	}
	stores := c.ext.resolveStores()
	if stores == nil || stores.Postgres == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "ad-hoc DDL not available (no relational store)"})
	}
	reqCtx := ctx.Request().Context()
	tid, _ := tenant.FromContext(reqCtx)
	// AUDIT: every ad-hoc DDL attempt is logged (it is outside the migration ledger).
	slog.Info("fabriq.adminapi.schema.adhoc_ddl", "tenant", tid, "sql", req.SQL)

	if err := stores.Postgres.ExecRawDDL(reqCtx, req.SQL); err != nil {
		return forge.BadRequest(err.Error())
	}
	return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "executed": req.SQL})
}
