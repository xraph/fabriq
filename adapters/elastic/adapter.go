// Package elastic is Fabriq's native Elasticsearch search adapter.
package elastic

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticsearch "github.com/elastic/go-elasticsearch/v9"

	"github.com/xraph/fabriq/adapters/search"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/internal/enginetrust"
)

// Config locates the Elasticsearch cluster. CACert supplies PEM trust roots
// for HTTPS endpoints.
type Config struct {
	Addrs    []string
	Username string
	Password string
	CACert   []byte
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
	if err := enginetrust.ValidateElasticsearch(cfg.Addrs, cfg.CACert); err != nil {
		return nil, err
	}
	esOpts := []elasticsearch.Option{elasticsearch.WithAddresses(cfg.Addrs...)}
	if cfg.Username != "" {
		esOpts = append(esOpts, elasticsearch.WithBasicAuth(cfg.Username, cfg.Password))
	}
	if len(cfg.CACert) != 0 {
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok || base == nil {
			return nil, errors.New("fabriq: elasticsearch inline CA requires an HTTP transport")
		}
		transport := base.Clone()
		// The native option fills RootCAs on this config. Use standard TLS
		// verification even if the host app configured its own TLS dialer.
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		transport.DialTLSContext = nil
		transport.DialTLS = nil //nolint:staticcheck // Clear the legacy hook so the native transport verifies certificates.
		esOpts = append(esOpts,
			elasticsearch.WithTransportOptions(elastictransport.WithTransport(transport)),
			elasticsearch.WithCACert(bytes.TrimSpace(cfg.CACert)),
		)
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
