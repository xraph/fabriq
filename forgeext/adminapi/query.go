package adminapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xraph/forge"
)

// queryRequest is the body for POST {BasePath}/query.
type queryRequest struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// queryResponse is a dynamic, arbitrary-column result set.
type queryResponse struct {
	Columns   []string         `json:"columns"`
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"rowCount"`
	Truncated bool             `json:"truncated"`
	ElapsedMs int64            `json:"elapsedMs"`
}

// precheckReadOnlySQL is a cheap, friendly early guard: the statement must be a
// single SELECT or WITH. It is defense-in-depth only — the read-only transaction
// in the adapter is the real enforcement, so this stays crude (no parser).
func precheckReadOnlySQL(sql string) error {
	s := strings.TrimSpace(sql)
	if s == "" {
		return fmt.Errorf("empty query")
	}
	// Reject statement stacking; allow a single trailing semicolon.
	if strings.Contains(strings.TrimSuffix(s, ";"), ";") {
		return fmt.Errorf("multiple statements are not allowed")
	}
	lower := strings.ToLower(s)
	if !strings.HasPrefix(lower, "select") && !strings.HasPrefix(lower, "with") {
		return fmt.Errorf("only read-only SELECT/WITH queries are allowed")
	}
	return nil
}

// registerQueryRoutes wires POST {BasePath}/query.
func (c *adminController) registerQueryRoutes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.query.raw"),
		forge.WithSummary("Run a read-only raw SQL query (body: {sql, args?}) → {columns, rows}"),
		forge.WithTags("Fabriq", "Admin"),
	}, c.ext.cfg.RouteOptions...)
	return r.POST(c.ext.cfg.BasePath+"/query", c.handleRawQuery, opts...)
}

// handleRawQuery serves POST {BasePath}/query. 400 on a non-read-only statement,
// tenant-guard trip, or SQL error; 501 when no relational store is opened.
func (c *adminController) handleRawQuery(ctx forge.Context) error {
	var req queryRequest
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		return forge.BadRequest("invalid request body: " + err.Error())
	}
	if err := precheckReadOnlySQL(req.SQL); err != nil {
		return forge.BadRequest(err.Error())
	}
	stores := c.ext.resolveStores()
	if stores == nil || stores.Postgres == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": errNoStores.Error()})
	}

	start := time.Now()
	rows, cols, truncated, err := stores.Postgres.QueryDynamicReadOnly(ctx.Request().Context(), req.SQL, req.Args...)
	if err != nil {
		// read-only violation, tenant-guard trip, SQL/column errors → 400.
		return forge.BadRequest(err.Error())
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	if cols == nil {
		cols = []string{}
	}
	return ctx.JSON(http.StatusOK, queryResponse{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
		ElapsedMs: time.Since(start).Milliseconds(),
	})
}
