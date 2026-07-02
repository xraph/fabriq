package client

import (
	"context"
	"fmt"
	"net/http"
)

// Client is a remote fabriq client bound to a single tenant/key over a
// transport (currently HTTP only).
type Client struct {
	baseURL string
	key     string
	tenant  string
	version int
	hc      *http.Client
}

// Option customizes a Client during Connect.
type Option func(*Client)

// WithHTTPClient overrides the *http.Client used for requests. Useful for
// tests (httptest) or custom transports/timeouts.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.hc = hc
		}
	}
}

// Connect parses dsn and returns a ready-to-use Client. Only the "http"
// transport is currently supported; "grpc" (and any other transport) is
// rejected with an error.
func Connect(ctx context.Context, dsn string, opts ...Option) (*Client, error) {
	_ = ctx // reserved for future connection-time handshake/ping

	d, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if d.Transport != "http" {
		return nil, fmt.Errorf("client: unsupported transport: %s", d.Transport)
	}

	c := &Client{
		baseURL: d.BaseURL(),
		key:     d.Key,
		tenant:  d.Tenant,
		version: d.Version,
		hc:      http.DefaultClient,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}
