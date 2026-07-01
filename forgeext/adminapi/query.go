package adminapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// errQueryNoStores is returned by the /query endpoint (not projections.go's
// errNoStores) so the 501 message reflects what actually failed here: no
// relational store opened for raw querying.
var errQueryNoStores = fmt.Errorf("raw query not available (no relational store opened)")

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
	if !hasKeywordPrefix(lower, "select") && !hasKeywordPrefix(lower, "with") {
		return fmt.Errorf("only read-only SELECT/WITH queries are allowed")
	}
	return nil
}

// hasKeywordPrefix reports whether s starts with keyword followed by a word
// boundary (whitespace, '(', or end-of-string) — so "selectfoo" and
// "withdraw" don't falsely match "select"/"with". Crude on purpose; the
// read-only transaction in the adapter is the real enforcement.
func hasKeywordPrefix(s, keyword string) bool {
	if !strings.HasPrefix(s, keyword) {
		return false
	}
	if len(s) == len(keyword) {
		return true
	}
	next := s[len(keyword)]
	if next == '(' {
		return true
	}
	isBoundaryChar := (next >= 'a' && next <= 'z') || (next >= '0' && next <= '9') || next == '_'
	return !isBoundaryChar
}

// fromJoinIdentRe matches an identifier in table position — right after a FROM
// or JOIN keyword — so rewriting is scoped to actual table references and never
// touches column refs, aliases, or string literals.
var fromJoinIdentRe = regexp.MustCompile(`(?i)(\bfrom\b|\bjoin\b)(\s+)("?)([a-zA-Z_][a-zA-Z0-9_]*)("?)`)

// sqlSkipRe matches regions the rewriter must treat as opaque: single-quoted
// string literals (with '' escapes), line comments, and block comments — so a
// word like "from products" inside a literal or comment is never rewritten.
var sqlSkipRe = regexp.MustCompile(`'(?:[^']|'')*'|--[^\n]*|/\*[\s\S]*?\*/`)

// resolveEntityTables rewrites bare entity-table references in FROM/JOIN
// positions to their physical ds_-prefixed table names, so callers can write
// `FROM customers` instead of `FROM ds_customers`. `physical` is the set of
// known physical dynamic-entity table names (lowercased). Identifiers already
// naming a physical table, or that don't resolve to a known table when
// ds_-prefixed (information_schema, a CTE name, a column/alias), are left
// untouched — and rewriting is skipped entirely inside string literals and
// comments, so it never alters a literal value or comment text.
func resolveEntityTables(sql string, physical map[string]bool) string {
	if len(physical) == 0 {
		return sql
	}
	var b strings.Builder
	last := 0
	for _, loc := range sqlSkipRe.FindAllStringIndex(sql, -1) {
		b.WriteString(rewriteFromJoinTables(sql[last:loc[0]], physical)) // code before the skip region
		b.WriteString(sql[loc[0]:loc[1]])                                // literal/comment, verbatim
		last = loc[1]
	}
	b.WriteString(rewriteFromJoinTables(sql[last:], physical))
	return b.String()
}

// rewriteFromJoinTables applies the FROM/JOIN table rewrite to one code segment.
func rewriteFromJoinTables(s string, physical map[string]bool) string {
	return fromJoinIdentRe.ReplaceAllStringFunc(s, func(m string) string {
		g := fromJoinIdentRe.FindStringSubmatch(m)
		kw, ws, q1, ident, q2 := g[1], g[2], g[3], g[4], g[5]
		low := strings.ToLower(ident)
		if physical[low] {
			return m // already a physical table name
		}
		if physical["ds_"+low] {
			return kw + ws + q1 + "ds_" + ident + q2
		}
		return m // unknown identifier — leave it alone
	})
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
// tenant-guard trip, or SQL error; 501 when no relational store is opened;
// 504 when the query is cancelled/times out (e.g. the statement_timeout).
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
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": errQueryNoStores.Error()})
	}

	if reg, rerr := c.ext.resolveRegistry(); rerr == nil {
		physical := make(map[string]bool)
		for _, ent := range reg.All() {
			if ent.Binding.IsDynamic() {
				physical[strings.ToLower(ent.Binding.Table)] = true
			}
		}
		req.SQL = resolveEntityTables(req.SQL, physical)
	}

	start := time.Now()
	rows, cols, truncated, err := stores.Postgres.QueryDynamicReadOnly(ctx.Request().Context(), req.SQL, req.Args...)
	if err != nil {
		if errors.Is(err, fabriqerr.ErrQueryTimeout) {
			return ctx.JSON(http.StatusGatewayTimeout, map[string]string{"error": "query exceeded the time limit"})
		}
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
