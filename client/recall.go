package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// RecallItem is a single hydrated, fused row in a hybrid-recall pack. It
// mirrors adminapi's recallItem JSON exactly:
// {entity, id, row, score, source, tokens}. Row is carried verbatim as a
// JSON object (json.RawMessage), not a quoted/escaped string. Source lists
// the channels that contributed the row to the fusion ("vector", "search",
// "graph").
type RecallItem struct {
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Row    json.RawMessage `json:"row"`
	Score  float64         `json:"score"`
	Source []string        `json:"source"`
	Tokens int             `json:"tokens"`
}

// RecallPack is the payload for Recall. It mirrors adminapi's
// recallResponse JSON exactly: {items, omitted, tokens, warnings}. Items
// are ordered best-first by fused score; Omitted counts rows that did not
// fit the budget; Tokens is the total used; Warnings carries per-channel
// degradation notes from the lenient recall pipeline.
type RecallPack struct {
	Items    []RecallItem `json:"items"`
	Omitted  int          `json:"omitted"`
	Tokens   int          `json:"tokens"`
	Warnings []string     `json:"warnings"`
}

// RecallRequest is the request body for Recall. It mirrors adminapi's
// recallRequest JSON exactly: {query, entities, budget, k, hops}.
type RecallRequest struct {
	// Query is the free-text recall query. Required; an empty query yields
	// an *APIError with Status 400.
	Query string `json:"query"`
	// Entities scopes recall to these dynamic entity types. When omitted the
	// server defaults to every registered dynamic (schema-backed) entity type.
	Entities []string `json:"entities,omitempty"`
	// Budget is the token budget for the assembled context pack. Zero defers
	// to the server default.
	Budget int `json:"budget,omitempty"`
	// K is the per-channel candidate count fed into RRF fusion. Zero defers
	// to the server default.
	K int `json:"k,omitempty"`
	// Hops is the graph-expansion depth for the graph channel. Zero defers
	// to the server default.
	Hops int `json:"hops,omitempty"`
}

// Recall runs the agent toolkit's hybrid-recall pipeline: per-channel
// candidate generation (vector, full-text search, graph expansion),
// Reciprocal Rank Fusion across the channels, authoritative relational
// hydration, and token-budget packing. It calls POST {BasePath}/recall.
// Returns an *APIError with Status 400 when Query is empty, or Status 501
// when hybrid recall is not configured (no embedder wired).
func (c *Client) Recall(ctx context.Context, req RecallRequest) (RecallPack, error) {
	var out RecallPack
	if err := c.do(ctx, http.MethodPost, "/recall", nil, req, &out); err != nil {
		return RecallPack{}, err
	}
	return out, nil
}

// WritePolicy is the agent write allowlist (deny-by-default): a map of
// entity name to permitted ops. It mirrors adminapi's writePolicyResponse
// JSON exactly: {allow}. An empty Allow map means every write is denied.
type WritePolicy struct {
	Allow map[string][]string `json:"allow"`
}

// GetWritePolicy reports the configured agent write allowlist. It calls
// GET {BasePath}/agent/write-policy.
func (c *Client) GetWritePolicy(ctx context.Context) (WritePolicy, error) {
	var out WritePolicy
	if err := c.do(ctx, http.MethodGet, "/agent/write-policy", nil, nil, &out); err != nil {
		return WritePolicy{}, err
	}
	return out, nil
}

// RememberInput is the request body for Remember. It mirrors adminapi's
// agent.RememberRequest JSON exactly: {entity, op, aggId, payload,
// expectedVersion} — the same shape as CommandInput.
type RememberInput struct {
	Entity string    `json:"entity"`
	Op     CommandOp `json:"op"`
	// AggID identifies the aggregate. Required for update/delete/upsert; a
	// ULID is minted server-side for create when omitted.
	AggID string `json:"aggId,omitempty"`
	// Payload is the column-keyed body (create/update/upsert). Ignored for delete.
	Payload map[string]any `json:"payload,omitempty"`
	// ExpectedVersion enables optimistic concurrency — a mismatch is a 409.
	ExpectedVersion *int64 `json:"expectedVersion,omitempty"`
}

// rememberResponse is the payload for Remember. It mirrors adminapi's
// rememberResponse JSON exactly: {result}.
type rememberResponse struct {
	Result CommandResult `json:"result"`
}

// Remember performs a policy-gated write through the agent toolkit. Power
// is WritePolicy ∩ tenant scope ∩ lifecycle-hook rules. It calls
// POST {BasePath}/agent/remember. Returns an *APIError whose Code mirrors
// the server's WriteError code and whose Status is mapped accordingly:
// validation_failed→400, not_allowed→403, version_conflict→409,
// not_found→404, else 500.
func (c *Client) Remember(ctx context.Context, input RememberInput) (CommandResult, error) {
	var out rememberResponse
	if err := c.do(ctx, http.MethodPost, "/agent/remember", nil, input, &out); err != nil {
		return CommandResult{}, err
	}
	return out.Result, nil
}
