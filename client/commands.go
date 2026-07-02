package client

import (
	"context"
	"net/http"
)

// CommandOp is a raw command verb against the write plane.
type CommandOp string

// Command verbs accepted by ExecCommand / ExecBatch.
const (
	CommandOpCreate CommandOp = "create"
	CommandOpUpdate CommandOp = "update"
	CommandOpDelete CommandOp = "delete"
	CommandOpUpsert CommandOp = "upsert"
)

// CommandInput is one raw command against the write plane. It mirrors
// adminapi's commandRequest JSON exactly:
// {entity, op, aggId, payload, expectedVersion}.
type CommandInput struct {
	// Entity is the registered entity name (e.g. "product").
	Entity string `json:"entity"`
	// Op is the command verb: create | update | delete | upsert.
	Op CommandOp `json:"op"`
	// AggID identifies the aggregate. Required for update/delete/upsert; a
	// ULID is minted server-side for create when omitted.
	AggID string `json:"aggId,omitempty"`
	// Payload is the column-keyed body (create/update/upsert). Ignored for delete.
	Payload map[string]any `json:"payload,omitempty"`
	// ExpectedVersion enables optimistic concurrency — a mismatch is a 409.
	ExpectedVersion *int64 `json:"expectedVersion,omitempty"`
}

// CommandResult is the outcome of one command. It mirrors adminapi's
// commandResultItem JSON exactly: {aggId, version, eventId}.
type CommandResult struct {
	AggID   string `json:"aggId"`
	Version int64  `json:"version"`
	EventID string `json:"eventId"`
}

// commandResponse is the payload for ExecCommand. It mirrors adminapi's
// commandResponse JSON exactly: {result}.
type commandResponse struct {
	Result CommandResult `json:"result"`
}

// commandBatchRequest is the request body for ExecBatch. It mirrors
// adminapi's commandBatchRequest JSON exactly: {commands}.
type commandBatchRequest struct {
	Commands []CommandInput `json:"commands"`
}

// commandBatchResponse is the payload for ExecBatch. It mirrors adminapi's
// commandBatchResponse JSON exactly: {results}.
type commandBatchResponse struct {
	Results []CommandResult `json:"results"`
}

// ExecCommand runs one command through the write plane. It calls
// POST {BasePath}/commands. Returns an *APIError with Status 400 on a
// malformed command, Status 404 on a missing aggregate (update/delete), or
// Status 409 on a version conflict.
func (c *Client) ExecCommand(ctx context.Context, cmd CommandInput) (CommandResult, error) {
	var out commandResponse
	if err := c.do(ctx, http.MethodPost, "/commands", nil, cmd, &out); err != nil {
		return CommandResult{}, err
	}
	return out.Result, nil
}

// ExecBatch runs N commands, ordered and all-or-nothing, in one transaction.
// It calls POST {BasePath}/commands/batch with body {commands}. Any
// command's failure rolls the whole batch back and is reported as that
// command's error (Status 400/404/409/500 via *APIError).
func (c *Client) ExecBatch(ctx context.Context, commands []CommandInput) ([]CommandResult, error) {
	var out commandBatchResponse
	body := commandBatchRequest{Commands: commands}
	if err := c.do(ctx, http.MethodPost, "/commands/batch", nil, body, &out); err != nil {
		return nil, err
	}
	return out.Results, nil
}
