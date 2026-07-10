package adminapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/xraph/forge"
)

// analyticsQuerier is an OPTIONAL read capability an analytics sink may
// implement: run a single read-only SELECT/WITH against the sink's own store
// and return a dynamic result set. When the configured sink does not implement
// it, POST /analytics/query answers 501 — the UI's signal to fall back.
type analyticsQuerier interface {
	QueryReadOnly(ctx context.Context, sql string, args ...any) (rows []map[string]any, cols []string, truncated bool, err error)
}

// analyticsQueryRequest is the POST {BasePath}/analytics/query body.
type analyticsQueryRequest struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// analyticsWriteKeywordRe matches a data-modifying keyword as a whole word.
// precheckReadOnlySQL only checks the leading keyword, so a data-modifying CTE
// (WITH x AS (DELETE ... RETURNING *) SELECT ...) would slip through; this
// denylist, applied to the literal/comment-stripped SQL, closes that vector.
// Crude, defense-in-depth: the analytics sink is a derived, reproducible read
// model, and the endpoint is gated on analytics.read.
var analyticsWriteKeywordRe = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|merge|vacuum)\b`)

// analyticsQueryTimeout bounds one analytics query.
const analyticsQueryTimeout = 15 * time.Second

// precheckAnalyticsReadOnly is precheckReadOnlySQL plus the write-keyword
// denylist on the literal/comment-stripped statement (see sqlSkipRe in query.go).
func precheckAnalyticsReadOnly(sql string) error {
	if err := precheckReadOnlySQL(sql); err != nil {
		return err
	}
	stripped := sqlSkipRe.ReplaceAllString(sql, " ")
	if analyticsWriteKeywordRe.MatchString(stripped) {
		return fmt.Errorf("only read-only queries are allowed (data-modifying keyword found)")
	}
	return nil
}

// handleAnalyticsQuery serves POST {BasePath}/analytics/query: a read-only SQL
// query over the analytics sink. 403 without analytics.read; 400 on a
// non-read-only statement; 501 when no sink is configured or the sink does not
// support querying; 504 on timeout; 200 {columns, rows, ...} otherwise.
func (c *adminController) handleAnalyticsQuery(ctx forge.Context) error {
	if err := c.requireAnalyticsRead(ctx); err != nil {
		return err
	}
	var body analyticsQueryRequest
	if derr := ctx.BindJSON(&body); derr != nil {
		return forge.BadRequest("invalid request body: " + derr.Error())
	}
	if perr := precheckAnalyticsReadOnly(body.SQL); perr != nil {
		return forge.BadRequest(perr.Error())
	}
	if c.ext.parent == nil || c.ext.parent.Stores() == nil || c.ext.parent.Stores().Analytics == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "analytics sink not configured"})
	}
	q, ok := c.ext.parent.Stores().Analytics.(analyticsQuerier)
	if !ok {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "analytics query not supported by this sink"})
	}

	qctx, cancel := context.WithTimeout(ctx.Request().Context(), analyticsQueryTimeout)
	defer cancel()
	start := time.Now()
	rows, cols, truncated, err := q.QueryReadOnly(qctx, body.SQL, body.Args...)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ctx.JSON(http.StatusGatewayTimeout, map[string]string{"error": "query exceeded the time limit"})
		}
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
