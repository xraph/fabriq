package client

import (
	"context"
	"net/http"
)

// QueryInput is the request body for RunQuery. It mirrors adminapi's
// queryRequest JSON exactly: {sql, args}.
type QueryInput struct {
	// SQL is a single read-only SELECT or WITH statement. Statement stacking
	// (multiple ;-separated statements) is rejected server-side.
	SQL string `json:"sql"`
	// Args are positional query parameters substituted into SQL.
	Args []any `json:"args,omitempty"`
}

// QueryResult is the payload for RunQuery. It mirrors adminapi's
// queryResponse JSON exactly: {columns, rows, rowCount, truncated, elapsedMs}.
type QueryResult struct {
	Columns   []string         `json:"columns"`
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"rowCount"`
	Truncated bool             `json:"truncated"`
	ElapsedMs int64            `json:"elapsedMs"`
}

// RunQuery runs a read-only raw SQL query for the current tenant. It calls
// POST {BasePath}/query with body {sql, args}. Returns an *APIError with
// Status 400 on a non-read-only statement, a tenant-guard trip, or a SQL
// error; Status 501 when no relational store is configured; Status 504 when
// the query is cancelled or exceeds the server's time limit.
func (c *Client) RunQuery(ctx context.Context, input QueryInput) (QueryResult, error) {
	var out QueryResult
	if err := c.do(ctx, http.MethodPost, "/query", nil, input, &out); err != nil {
		return QueryResult{}, err
	}
	return out, nil
}
