package client

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// DigestNode is a single node in the per-tenant context-distillation Merkle
// tree (the "AI data fabric"). It mirrors adminapi's distillNode JSON
// exactly: {id, level, kind, scope, contentHash, semHash, summary}. Level 2
// is the tenant root, level 1 is a scope/cluster node, level 0 is an entity
// leaf. Scope and Summary are omitted by the server when empty.
type DigestNode struct {
	ID    string `json:"id"`
	Level int    `json:"level"`
	Kind  string `json:"kind"`
	// Scope is the scope value an L1 scope node summarizes (empty otherwise).
	Scope string `json:"scope,omitempty"`
	// ContentHash is the Merkle freshness key (changes when the subtree changes).
	ContentHash string `json:"contentHash"`
	// SemHash is the 16-hex SimHash fingerprint of the node's summary embedding.
	SemHash string `json:"semHash"`
	// Summary is the node's summary text when surfaced; usually empty in the
	// map (see DigestMap) — use GetDigestNode for full text.
	Summary string `json:"summary,omitempty"`
}

// DigestMap is the payload for GetDigestMap. It mirrors adminapi's
// distillMapResponse JSON exactly: {rootId, nodes}. Nodes is empty
// (non-nil) when the tenant has no digest data yet.
type DigestMap struct {
	// RootID is the stable id of the tenant (L2) root: "digest:2:tenant".
	RootID string       `json:"rootId"`
	Nodes  []DigestNode `json:"nodes"`
}

// DigestChild is one immediate child of a digest node, as returned by
// GetDigestNode. It mirrors adminapi's distillChild JSON exactly:
// {id, kind, summary, contentHash, semHash}.
type DigestChild struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Summary     string `json:"summary"`
	ContentHash string `json:"contentHash"`
	SemHash     string `json:"semHash"`
}

// DigestView is the payload for GetDigestNode. It mirrors adminapi's
// distillNodeResponse JSON exactly: {node, summary, children}.
type DigestView struct {
	Node     DigestNode    `json:"node"`
	Summary  string        `json:"summary"`
	Children []DigestChild `json:"children"`
}

// GetDigestMap fetches the full, deterministically-sorted outline of the
// tenant's context-distillation Merkle tree. It calls
// GET {BasePath}/distill/map. When the distillation plane is not configured
// (digest_node not registered) the server returns 200 with an empty node
// list rather than an error.
func (c *Client) GetDigestMap(ctx context.Context) (DigestMap, error) {
	var out DigestMap
	if err := c.do(ctx, http.MethodGet, "/distill/map", nil, nil, &out); err != nil {
		return DigestMap{}, err
	}
	return out, nil
}

// GetDigestNode drills into one digest node: its own outline line, its
// summary text (from CAS; empty when no CAS is configured), and its
// immediate children with their hashes and summaries. It calls
// GET {BasePath}/distill/node/:id. Returns an *APIError with Status 501
// when the distillation plane is not configured, or Status 404 when the id
// names no node in the tenant's tree.
//
// id is colon-delimited (e.g. "digest:2:tenant"); each ":"-separated segment
// is percent-escaped individually so a value inside a segment cannot collide
// with the delimiter, then the segments are re-joined with ":" — mirroring
// the fabriq-admin TS client's distillNode encoding.
func (c *Client) GetDigestNode(ctx context.Context, id string) (DigestView, error) {
	var out DigestView
	if err := c.do(ctx, http.MethodGet, "/distill/node/"+encodeDigestID(id), nil, nil, &out); err != nil {
		return DigestView{}, err
	}
	return out, nil
}

// encodeDigestID percent-escapes each ":"-separated segment of a digest node
// id individually, then re-joins them with ":", so the colons that delimit
// the id's levels are preserved as literal colons in the path rather than
// being escaped to %3A. Mirrors the fabriq-admin TS client's distillNode id
// encoding.
func encodeDigestID(id string) string {
	segments := strings.Split(id, ":")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, ":")
}
