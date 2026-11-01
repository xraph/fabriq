// Package search shares projection and query semantics across native search transports.
package search

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/xraph/fabriq/core/registry"
)

// Transport is the native client's HTTP request port.
type Transport interface {
	Perform(*http.Request) (*http.Response, error)
}

// ModelVersionResolver reports the live search model version for a tenant.
type ModelVersionResolver func(context.Context, string) (int, error)

// Adapter implements the shared search query and projection behavior.
type Adapter struct {
	transport      Transport
	reg            *registry.Registry
	modelVersion   ModelVersionResolver
	transportError func(string, error) error
	responseError  func(string, int, string) error
	mu             sync.Mutex
	ensured        map[string]struct{}
	aliased        map[string]struct{}
}

// New assembles search behavior over an authenticated native engine transport.
func New(t Transport, reg *registry.Registry, resolver ModelVersionResolver, transportError func(string, error) error, responseError func(string, int, string) error) *Adapter {
	if resolver == nil {
		resolver = func(context.Context, string) (int, error) { return 1, nil }
	}
	return &Adapter{transport: t, reg: reg, modelVersion: resolver, transportError: transportError, responseError: responseError, ensured: map[string]struct{}{}, aliased: map[string]struct{}{}}
}

func (a *Adapter) request(ctx context.Context, method, path, params string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	req.URL.RawQuery = params
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		if path == "/_bulk" {
			req.Header.Set("Content-Type", "application/x-ndjson")
		}
	}
	return a.transport.Perform(req)
}

// Ping reports engine reachability, bounded by ctx.
func (a *Adapter) Ping(ctx context.Context) error {
	res, err := a.request(ctx, "GET", "/", "", nil)
	if err != nil {
		return a.transportError("search ping", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, res.Body); _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		return a.responseError("search ping", res.StatusCode, responseBody(res))
	}
	return nil
}

func responseBody(res *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	return string(body)
}
func isVersionConflict(item map[string]any) bool {
	errObj, _ := item["error"].(map[string]any)
	t, _ := errObj["type"].(string)
	return strings.Contains(t, "version_conflict")
}
