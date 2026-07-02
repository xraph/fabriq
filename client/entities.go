package client

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// EntityRecord is a single dynamic-entity row, as returned by the entity
// list and detail endpoints. It mirrors adminapi's entityItem JSON exactly:
// {id, type, data}.
type EntityRecord struct {
	ID   string         `json:"id"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

// EntityPage is a page of EntityRecord results, as returned by
// GET {BasePath}/entities. It mirrors adminapi's entityListResponse JSON
// exactly: {items, nextCursor}.
type EntityPage struct {
	Items      []EntityRecord `json:"items"`
	NextCursor string         `json:"nextCursor"`
}

// EntityWriteInput is the request body for creating or updating an entity
// row. It mirrors adminapi's entityWriteRequest JSON exactly: {type, data}.
type EntityWriteInput struct {
	// Type is the registered dynamic entity type name (e.g. "product").
	Type string `json:"type"`
	// Data is the column-keyed payload written to the row.
	Data map[string]any `json:"data"`
}

// ListEntitiesParams are the query parameters for ListEntities. Type is
// required by the backend; Limit and Cursor are optional.
type ListEntitiesParams struct {
	// Type is the registered dynamic entity type name (e.g. "product").
	Type string
	// Limit caps the page size (server default 50, max 200). Zero omits the
	// query param and defers to the server default.
	Limit int
	// Cursor resumes a previous listing (server emits this as
	// EntityPage.NextCursor). Empty starts from the first page.
	Cursor string
}

// ListEntities lists rows of a dynamic entity type. It calls
// GET {BasePath}/entities?type=<type>[&limit=<n>][&cursor=<c>].
func (c *Client) ListEntities(ctx context.Context, params ListEntitiesParams) (EntityPage, error) {
	q := url.Values{}
	if params.Type != "" {
		q.Set("type", params.Type)
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Cursor != "" {
		q.Set("cursor", params.Cursor)
	}

	var out EntityPage
	if err := c.do(ctx, http.MethodGet, "/entities", q, nil, &out); err != nil {
		return EntityPage{}, err
	}
	return out, nil
}

// GetEntity fetches a single entity row by id. It calls
// GET {BasePath}/entities/:id?type=<type>. Type is required by the backend.
func (c *Client) GetEntity(ctx context.Context, id, entityType string) (EntityRecord, error) {
	q := url.Values{}
	if entityType != "" {
		q.Set("type", entityType)
	}

	var out EntityRecord
	if err := c.do(ctx, http.MethodGet, "/entities/"+url.PathEscape(id), q, nil, &out); err != nil {
		return EntityRecord{}, err
	}
	return out, nil
}

// CreateEntity creates a new entity row. It calls POST {BasePath}/entities
// with body {type, data}; the id is generated server-side.
func (c *Client) CreateEntity(ctx context.Context, input EntityWriteInput) (EntityRecord, error) {
	var out EntityRecord
	if err := c.do(ctx, http.MethodPost, "/entities", nil, input, &out); err != nil {
		return EntityRecord{}, err
	}
	return out, nil
}

// UpdateEntity replaces an existing entity row's domain columns. It calls
// PUT {BasePath}/entities/:id with body {type, data}.
func (c *Client) UpdateEntity(ctx context.Context, id string, input EntityWriteInput) (EntityRecord, error) {
	var out EntityRecord
	if err := c.do(ctx, http.MethodPut, "/entities/"+url.PathEscape(id), nil, input, &out); err != nil {
		return EntityRecord{}, err
	}
	return out, nil
}

// DeleteEntity deletes an entity row by id. It calls
// DELETE {BasePath}/entities/:id?type=<type>. Type is required by the backend.
func (c *Client) DeleteEntity(ctx context.Context, id, entityType string) error {
	q := url.Values{}
	if entityType != "" {
		q.Set("type", entityType)
	}

	return c.do(ctx, http.MethodDelete, "/entities/"+url.PathEscape(id), q, nil, nil)
}
