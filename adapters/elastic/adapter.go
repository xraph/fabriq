// Package elastic is Fabriq's native Elasticsearch search adapter.
package elastic

import (
	"context"
	"fmt"

	elasticsearch "github.com/elastic/go-elasticsearch/v9"

	"github.com/xraph/fabriq/adapters/search"
	"github.com/xraph/fabriq/core/registry"
)

// Config locates the Elasticsearch cluster.
type Config struct {
	Addrs    []string
	Username string
	Password string
}

// ModelVersionResolver reports the live search model version for a tenant.
type ModelVersionResolver = search.ModelVersionResolver

// Adapter implements the search query and projection ports using Elasticsearch.
type Adapter struct {
	*search.Adapter
	modelVersion ModelVersionResolver
	client       *elasticsearch.Client
}

// Option customizes the adapter.
type Option func(*Adapter)

// WithModelVersionResolver wires the projection-state-backed live version.
func WithModelVersionResolver(fn ModelVersionResolver) Option {
	return func(a *Adapter) {
		if fn != nil {
			a.modelVersion = fn
		}
	}
}

// Open connects to Elasticsearch using its native product-checked transport.
func Open(ctx context.Context, cfg Config, reg *registry.Registry, opts ...Option) (*Adapter, error) {
	esOpts := []elasticsearch.Option{elasticsearch.WithAddresses(cfg.Addrs...)}
	if cfg.Username != "" {
		esOpts = append(esOpts, elasticsearch.WithBasicAuth(cfg.Username, cfg.Password))
	}
	client, err := elasticsearch.New(esOpts...)
	if err != nil {
		return nil, fmt.Errorf("fabriq: elasticsearch client: %w", err)
	}
	a := &Adapter{client: client}
	for _, opt := range opts {
		opt(a)
	}
	a.Adapter = search.New(client, reg, a.modelVersion, translateES, translateESResponse)
	if err := a.Ping(ctx); err != nil {
		_ = client.Close(context.Background())
		return nil, err
	}
	return a, nil
}

// SearchTargetName names the search projection's versioned rebuild target.
func SearchTargetName(tenantID string, version int) string {
	return search.TargetName(tenantID, version)
}

// Close releases the native client's background resources and connections.
func (a *Adapter) Close() error { return a.client.Close(context.Background()) }
