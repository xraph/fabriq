package client

import (
	"context"
	"net/http"
	"net/url"
)

// MigrationInfo is one migration's status. It mirrors adminapi's
// migrationItem JSON exactly: {name, version, group, comment, applied, appliedAt}.
type MigrationInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Group   string `json:"group"`
	Comment string `json:"comment"`
	Applied bool   `json:"applied"`
	// AppliedAt is empty when the migration has not been applied.
	AppliedAt string `json:"appliedAt,omitempty"`
}

// MigrationGroupStatus is one migration group's applied/pending status. It
// mirrors adminapi's migrationGroup JSON exactly: {name, applied, pending}.
type MigrationGroupStatus struct {
	Name    string          `json:"name"`
	Applied []MigrationInfo `json:"applied"`
	Pending []MigrationInfo `json:"pending"`
}

// MigrationStatus is the payload for GetMigrationStatus. It mirrors
// adminapi's migrationStatusResponse JSON exactly: {groups}.
type MigrationStatus struct {
	Groups []MigrationGroupStatus `json:"groups"`
}

// MigrationJobHandle is the payload for RunMigrations and RollbackMigrations.
// It mirrors adminapi's response JSON exactly: {jobId}.
type MigrationJobHandle struct {
	JobID string `json:"jobId"`
}

// MigrationJob is one async migration run (up or down), as returned by
// GetMigrationJob. It mirrors adminapi's migrationJob JSON exactly:
// {id, kind, state, names, error, startedAt, endedAt}.
type MigrationJob struct {
	ID string `json:"id"`
	// Kind is "up" or "down".
	Kind string `json:"kind"`
	// State is "running", "done", or "failed".
	State     string   `json:"state"`
	Names     []string `json:"names,omitempty"`
	Error     string   `json:"error,omitempty"`
	StartedAt string   `json:"startedAt"`
	// EndedAt is empty while the job is running.
	EndedAt string `json:"endedAt,omitempty"`
}

// MigrationScaffoldInput is the request body for ScaffoldMigration. It
// mirrors adminapi's scaffoldRequest JSON exactly: {name, version, up, down}.
type MigrationScaffoldInput struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	// Up/Down are optional forward/reverse DDL statements (one per element);
	// omitted, the generated file carries TODO placeholders.
	Up   []string `json:"up,omitempty"`
	Down []string `json:"down,omitempty"`
}

// MigrationScaffold is the payload for ScaffoldMigration. It mirrors
// adminapi's response JSON exactly: {filename, content}.
type MigrationScaffold struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
}

// DriftEntity is one entity's registry-vs-physical schema drift. It mirrors
// adminapi's driftEntity JSON exactly:
// {entity, table, dynamic, inSync, missing, extra, error}.
type DriftEntity struct {
	Entity  string `json:"entity"`
	Table   string `json:"table"`
	Dynamic bool   `json:"dynamic"`
	InSync  bool   `json:"inSync"`
	// Missing lists columns expected by the registry but absent physically.
	Missing []string `json:"missing"`
	// Extra lists columns present physically but not in the registry.
	Extra []string `json:"extra"`
	// Error is set when this entity's table could not be introspected.
	Error string `json:"error,omitempty"`
}

// SchemaDrift is the payload for GetSchemaDrift. It mirrors adminapi's
// driftResponse JSON exactly: {entities}.
type SchemaDrift struct {
	Entities []DriftEntity `json:"entities"`
}

// DDLResult is the payload for RunDDL. It mirrors adminapi's response JSON
// exactly: {ok, executed}.
type DDLResult struct {
	OK       bool   `json:"ok"`
	Executed string `json:"executed"`
}

// GetMigrationStatus lists applied and pending migrations, grouped. It calls
// GET {BasePath}/migrations. This is always available (no schema-admin
// gate) — it only reports state, it never executes migrations. Returns an
// *APIError with Status 501 when no migration target is configured.
func (c *Client) GetMigrationStatus(ctx context.Context) (MigrationStatus, error) {
	var out MigrationStatus
	if err := c.do(ctx, http.MethodGet, "/migrations", nil, nil, &out); err != nil {
		return MigrationStatus{}, err
	}
	return out, nil
}

// RunMigrations runs all pending migrations as a background job. It calls
// POST {BasePath}/migrations/up. Poll the returned job id with
// GetMigrationJob. Returns an *APIError with Status 403 when schema-admin is
// not enabled, Status 409 when another migration run is already in flight,
// or Status 501 when migration execution is unavailable.
func (c *Client) RunMigrations(ctx context.Context) (MigrationJobHandle, error) {
	var out MigrationJobHandle
	if err := c.do(ctx, http.MethodPost, "/migrations/up", nil, nil, &out); err != nil {
		return MigrationJobHandle{}, err
	}
	return out, nil
}

// RollbackMigrations rolls back the last applied migration batch as a
// background job. It calls POST {BasePath}/migrations/down. Poll the
// returned job id with GetMigrationJob. Returns an *APIError with Status 403
// when schema-admin is not enabled, Status 409 when another migration run is
// already in flight, or Status 501 when migration execution is unavailable.
func (c *Client) RollbackMigrations(ctx context.Context) (MigrationJobHandle, error) {
	var out MigrationJobHandle
	if err := c.do(ctx, http.MethodPost, "/migrations/down", nil, nil, &out); err != nil {
		return MigrationJobHandle{}, err
	}
	return out, nil
}

// GetMigrationJob polls one migration job's state. It calls
// GET {BasePath}/migrations/jobs/:id. Returns an *APIError with Status 404
// when no such job exists.
func (c *Client) GetMigrationJob(ctx context.Context, id string) (MigrationJob, error) {
	var out MigrationJob
	if err := c.do(ctx, http.MethodGet, "/migrations/jobs/"+url.PathEscape(id), nil, nil, &out); err != nil {
		return MigrationJob{}, err
	}
	return out, nil
}

// MigrationJobStreamPath returns the request path for the SSE stream of a
// migration job's state (GET {BasePath}/migrations/jobs/:id/stream). It does
// not issue a request — the caller opens a streaming/SSE connection to
// BaseURL()+this path directly, since the client's do() helper only supports
// request/response JSON calls.
func (c *Client) MigrationJobStreamPath(id string) string {
	return "/migrations/jobs/" + url.PathEscape(id) + "/stream"
}

// ScaffoldMigration generates a grove Go migration-file skeleton. It runs
// nothing and writes nothing — it only returns text for a developer to save
// into migrations/ and register in module.go. It calls
// POST {BasePath}/migrations/scaffold. Returns an *APIError with Status 403
// when schema-admin is not enabled, or Status 400 on an invalid name/version.
func (c *Client) ScaffoldMigration(ctx context.Context, input MigrationScaffoldInput) (MigrationScaffold, error) {
	var out MigrationScaffold
	if err := c.do(ctx, http.MethodPost, "/migrations/scaffold", nil, input, &out); err != nil {
		return MigrationScaffold{}, err
	}
	return out, nil
}

// GetSchemaDrift reports, per registered entity, the registry-vs-physical
// schema drift: columns the registry expects that are missing physically,
// and physical columns not in the registry. It calls
// GET {BasePath}/schema/drift. Returns an *APIError with Status 501 when no
// relational store is configured.
func (c *Client) GetSchemaDrift(ctx context.Context) (SchemaDrift, error) {
	var out SchemaDrift
	if err := c.do(ctx, http.MethodGet, "/schema/drift", nil, nil, &out); err != nil {
		return SchemaDrift{}, err
	}
	return out, nil
}

// RunDDL runs a single ad-hoc DDL statement as the schema owner. It is NOT
// recorded in the migration ledger — this is a gated escape hatch deliberately
// outside the migration authority. It calls POST {BasePath}/schema/ddl with
// body {sql}. Returns an *APIError with Status 403 when schema-admin is not
// enabled, Status 400 on a bad/multi statement or a SQL error (the raw
// database error is surfaced, since this caller is already privileged), or
// Status 501 when no relational store is configured.
func (c *Client) RunDDL(ctx context.Context, sql string) (DDLResult, error) {
	var out DDLResult
	body := map[string]string{"sql": sql}
	if err := c.do(ctx, http.MethodPost, "/schema/ddl", nil, body, &out); err != nil {
		return DDLResult{}, err
	}
	return out, nil
}
