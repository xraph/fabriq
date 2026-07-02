package client

import (
	"context"
	"net/http"
	"net/url"
)

// SchemaField is one field descriptor in a dynamic entity's schema. It
// mirrors adminapi's schemaField JSON exactly: {name, kind, required}.
type SchemaField struct {
	// Name is the column name.
	Name string `json:"name"`
	// Kind is the simplified field kind: string, number, boolean, time, or object.
	Kind string `json:"kind"`
	// Required reports whether the column is NOT NULL.
	Required bool `json:"required"`
}

// EntitySchema is the payload for GetEntitySchema and the schema-mutating
// methods. It mirrors adminapi's schemaResponse JSON exactly: {type, fields}.
type EntitySchema struct {
	Type   string        `json:"type"`
	Fields []SchemaField `json:"fields"`
}

// SchemaColumnInput describes a column when defining or extending a dynamic
// entity type's schema. It mirrors adminapi's schemaWriteColumn JSON
// exactly: {name, kind, required, default}.
type SchemaColumnInput struct {
	Name string `json:"name"`
	// Kind is one of: string, number, boolean, time, object.
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	// Default is a SQL default expression. Allowed forms: a number,
	// true/false/null, now(), or a single-quoted string literal. Empty omits
	// the field.
	Default string `json:"default,omitempty"`
}

// SchemaIndexInput describes an index when defining or extending a dynamic
// entity type's schema. It mirrors adminapi's schemaWriteIndex JSON exactly:
// {name, columns, unique}.
type SchemaIndexInput struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
}

// CreateEntityTypeInput is the request body for CreateEntityType. It mirrors
// adminapi's defineSchemaRequest JSON exactly: {type, columns, indexes}.
type CreateEntityTypeInput struct {
	Type    string              `json:"type"`
	Columns []SchemaColumnInput `json:"columns"`
	Indexes []SchemaIndexInput  `json:"indexes,omitempty"`
}

// AddEntityFieldsInput is the request body for AddEntityFields. It mirrors
// adminapi's addFieldsRequest JSON exactly: {columns, indexes}.
type AddEntityFieldsInput struct {
	Columns []SchemaColumnInput `json:"columns"`
	Indexes []SchemaIndexInput  `json:"indexes,omitempty"`
}

// entityTypesEnvelope unwraps adminapi's entityTypesResponse JSON: {types}.
type entityTypesEnvelope struct {
	Types []string `json:"types"`
}

// ListEntityTypes lists the registered dynamic entity type names. It calls
// GET {BasePath}/entities/types.
func (c *Client) ListEntityTypes(ctx context.Context) ([]string, error) {
	var env entityTypesEnvelope
	if err := c.do(ctx, http.MethodGet, "/entities/types", nil, nil, &env); err != nil {
		return nil, err
	}
	if env.Types == nil {
		return []string{}, nil
	}
	return env.Types, nil
}

// GetEntitySchema fetches a dynamic entity type's field descriptors. It
// calls GET {BasePath}/schema?type=<entityType>. Returns an *APIError with
// Status 400 when the type is unknown or not dynamic.
func (c *Client) GetEntitySchema(ctx context.Context, entityType string) (EntitySchema, error) {
	q := url.Values{}
	q.Set("type", entityType)

	var out EntitySchema
	if err := c.do(ctx, http.MethodGet, "/schema", q, nil, &out); err != nil {
		return EntitySchema{}, err
	}
	return out, nil
}

// CreateEntityType defines a new dynamic entity type and creates its backing
// table. It calls POST {BasePath}/schema with body {type, columns, indexes}.
// Returns an *APIError with Status 400 on an invalid column kind or default
// expression, Status 409 when the type is already registered, or Status 501
// when no relational store is configured.
func (c *Client) CreateEntityType(ctx context.Context, input CreateEntityTypeInput) (EntitySchema, error) {
	var out EntitySchema
	if err := c.do(ctx, http.MethodPost, "/schema", nil, input, &out); err != nil {
		return EntitySchema{}, err
	}
	return out, nil
}

// AddEntityFields adds columns (and optional indexes) to an existing dynamic
// entity type. It calls POST {BasePath}/schema/:type/fields with body
// {columns, indexes}. Returns an *APIError with Status 404 when the type is
// unknown.
func (c *Client) AddEntityFields(ctx context.Context, entityType string, input AddEntityFieldsInput) (EntitySchema, error) {
	var out EntitySchema
	if err := c.do(ctx, http.MethodPost, "/schema/"+url.PathEscape(entityType)+"/fields", nil, input, &out); err != nil {
		return EntitySchema{}, err
	}
	return out, nil
}

// RenameEntityField renames a column on an existing dynamic entity type. It
// calls POST {BasePath}/schema/:type/rename-field with body {from, to}.
func (c *Client) RenameEntityField(ctx context.Context, entityType, from, to string) error {
	body := map[string]string{"from": from, "to": to}
	return c.do(ctx, http.MethodPost, "/schema/"+url.PathEscape(entityType)+"/rename-field", nil, body, nil)
}

// DropEntityField drops a column from a dynamic entity type. It calls
// DELETE {BasePath}/schema/:type/fields/:column?confirm=<column> (the confirm
// query param guards accidental drops and is set automatically).
func (c *Client) DropEntityField(ctx context.Context, entityType, column string) error {
	q := url.Values{}
	q.Set("confirm", column)
	return c.do(ctx, http.MethodDelete, "/schema/"+url.PathEscape(entityType)+"/fields/"+url.PathEscape(column), q, nil, nil)
}

// DeleteEntityType drops a dynamic entity type entirely, including its
// backing table. It calls DELETE {BasePath}/schema/:type?confirm=<type> (the
// confirm query param guards accidental drops and is set automatically).
func (c *Client) DeleteEntityType(ctx context.Context, entityType string) error {
	q := url.Values{}
	q.Set("confirm", entityType)
	return c.do(ctx, http.MethodDelete, "/schema/"+url.PathEscape(entityType), q, nil, nil)
}
