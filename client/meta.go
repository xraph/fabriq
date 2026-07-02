package client

import (
	"context"
	"net/http"
	"net/url"
)

// Meta is the payload for GetMeta. It mirrors adminapi's metaResponse JSON
// exactly: {name, version, capabilities, tenant}.
type Meta struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
	// Tenant is the resolved tenant id echoed back when X-Tenant-ID is sent.
	// Empty for unauthenticated or tenant-agnostic callers.
	Tenant string `json:"tenant,omitempty"`
}

// InstanceCapabilities reports which fabric subsystems this fabriq instance
// has configured. It mirrors adminapi's instanceCapabilities JSON exactly:
// {relational, graph, vector, spatial, search, crdt, files, distill, timeseries}.
type InstanceCapabilities struct {
	Relational bool `json:"relational"`
	Graph      bool `json:"graph"`
	Vector     bool `json:"vector"`
	Spatial    bool `json:"spatial"`
	Search     bool `json:"search"`
	CRDT       bool `json:"crdt"`
	Files      bool `json:"files"`
	Distill    bool `json:"distill"`
	Timeseries bool `json:"timeseries"`
}

// TypeCapabilities reports which subsystems a single dynamic entity type
// participates in. It mirrors adminapi's typeCapabilities JSON exactly:
// {relational, vector, search, spatial, crdt, graph}.
type TypeCapabilities struct {
	Relational bool `json:"relational"`
	Vector     bool `json:"vector"`
	Search     bool `json:"search"`
	Spatial    bool `json:"spatial"`
	CRDT       bool `json:"crdt"`
	Graph      bool `json:"graph"`
}

// TypeCapabilitiesResult is the payload for GetTypeCapabilities. It mirrors
// adminapi's typeCapabilitiesResponse JSON exactly: {type, capabilities}.
type TypeCapabilitiesResult struct {
	Type         string           `json:"type"`
	Capabilities TypeCapabilities `json:"capabilities"`
}

// instanceCapabilitiesEnvelope unwraps adminapi's instanceCapabilitiesResponse
// JSON: {capabilities}.
type instanceCapabilitiesEnvelope struct {
	Capabilities InstanceCapabilities `json:"capabilities"`
}

// GetMeta fetches the admin API's metadata: its name, version, the static
// capability-string list, and the resolved tenant (when present). It calls
// GET {BasePath}/meta.
func (c *Client) GetMeta(ctx context.Context) (Meta, error) {
	var out Meta
	if err := c.do(ctx, http.MethodGet, "/meta", nil, nil, &out); err != nil {
		return Meta{}, err
	}
	return out, nil
}

// GetInstanceCapabilities reports which fabric subsystems this fabriq
// instance has configured (relational, graph, vector, spatial, search, crdt,
// files, distill, timeseries). It calls GET {BasePath}/capabilities (no
// ?type=).
func (c *Client) GetInstanceCapabilities(ctx context.Context) (InstanceCapabilities, error) {
	var env instanceCapabilitiesEnvelope
	if err := c.do(ctx, http.MethodGet, "/capabilities", nil, nil, &env); err != nil {
		return InstanceCapabilities{}, err
	}
	return env.Capabilities, nil
}

// GetTypeCapabilities reports which subsystems a single dynamic entity type
// participates in, read from its declarative registry EntitySpec. It calls
// GET {BasePath}/capabilities?type=<entityType>. Returns an *APIError with
// Status 400 when the type is unknown.
func (c *Client) GetTypeCapabilities(ctx context.Context, entityType string) (TypeCapabilitiesResult, error) {
	q := url.Values{}
	q.Set("type", entityType)

	var out TypeCapabilitiesResult
	if err := c.do(ctx, http.MethodGet, "/capabilities", q, nil, &out); err != nil {
		return TypeCapabilitiesResult{}, err
	}
	return out, nil
}
