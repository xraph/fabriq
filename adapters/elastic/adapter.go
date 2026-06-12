// Package elastic is fabriq's search adapter on go-elasticsearch.
//
// Tenancy and blue-green routing live in index naming, derived solely
// from core/registry: reads hit the per-tenant ALIAS
// (fabriq_{tenant}_{base}), writes hit the versioned index behind it
// (fabriq_{tenant}_{base}_v{N}); rebuilds build _v{N+1} and swap the
// alias atomically. Idempotency is engine-side: every bulk op carries the
// aggregate version with version_type=external_gte, so stale replays are
// version conflicts (treated as success).
package elastic

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	elasticsearch "github.com/elastic/go-elasticsearch/v9"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// Config locates the Elasticsearch cluster.
type Config struct {
	Addrs    []string
	Username string
	Password string
}

// ModelVersionResolver reports the live search model version for a tenant
// (projection_state-backed in production; defaults to 1).
type ModelVersionResolver func(ctx context.Context, tenantID string) (int, error)

// Adapter implements query.SearchQuerier.
type Adapter struct {
	es           *elasticsearch.Client
	reg          *registry.Registry
	modelVersion ModelVersionResolver

	mu      sync.Mutex
	ensured map[string]struct{} // index names already created
	aliased map[string]struct{} // aliases already pointed
}

var _ query.SearchQuerier = (*Adapter)(nil)

// Option customizes the adapter.
type Option func(*Adapter)

// WithModelVersionResolver wires the projection_state-backed live version.
func WithModelVersionResolver(fn ModelVersionResolver) Option {
	return func(a *Adapter) {
		if fn != nil {
			a.modelVersion = fn
		}
	}
}

// Open dials Elasticsearch and pings it.
func Open(_ context.Context, cfg Config, reg *registry.Registry, opts ...Option) (*Adapter, error) {
	esOpts := []elasticsearch.Option{elasticsearch.WithAddresses(cfg.Addrs...)}
	if cfg.Username != "" {
		esOpts = append(esOpts, elasticsearch.WithBasicAuth(cfg.Username, cfg.Password))
	}
	client, err := elasticsearch.New(esOpts...)
	if err != nil {
		return nil, fmt.Errorf("fabriq: elasticsearch client: %w", err)
	}
	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("fabriq: elasticsearch ping: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("fabriq: elasticsearch ping: %s", res.String())
	}
	a := &Adapter{
		es:           client,
		reg:          reg,
		modelVersion: func(context.Context, string) (int, error) { return 1, nil },
		ensured:      map[string]struct{}{},
		aliased:      map[string]struct{}{},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

// drainAndClose consumes a response body so the connection is reusable.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

func isVersionConflict(item map[string]any) bool {
	errObj, _ := item["error"].(map[string]any)
	t, _ := errObj["type"].(string)
	return strings.Contains(t, "version_conflict")
}
