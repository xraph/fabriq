// Package opensearch implements Fabriq's search projection over native OpenSearch.
package opensearch

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"

	native "github.com/opensearch-project/opensearch-go/v4"

	"github.com/xraph/fabriq/adapters/search"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
)

// Config locates an OpenSearch cluster. CACert adds PEM trust roots; CAFile
// reads those roots from disk. TLS certificate and hostname checks stay enabled.
type Config struct {
	Addrs    []string
	Username string
	Password string
	CACert   []byte
	CAFile   string
}

// Validate checks addresses and inline trust without reading files or dialing.
func (c Config) Validate() error {
	if len(c.CACert) > 0 && (c.CAFile != "" || len(c.Addrs) == 0) {
		return safeError("opensearch CA config", fabriqerr.CodeInvalidInput)
	}
	for _, addr := range c.Addrs {
		u, err := url.Parse(addr)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return safeError("opensearch config", fabriqerr.CodeInvalidInput)
		}
		if len(c.CACert) > 0 && u.Scheme != "https" {
			return safeError("opensearch CA requires HTTPS", fabriqerr.CodeInvalidInput)
		}
	}
	if len(c.CACert) > 0 && !validInlineCA(c.CACert) {
		return safeError("opensearch CA certificate", fabriqerr.CodeInvalidInput)
	}
	return nil
}

func validInlineCA(data []byte) bool {
	if len(data) > 64<<10 {
		return false
	}
	const begin = "-----BEGIN CERTIFICATE-----"
	const end = "-----END CERTIFICATE-----"
	count := 0
	for data = bytes.TrimSpace(data); len(data) > 0; data = bytes.TrimSpace(data) {
		if !bytes.HasPrefix(data, []byte(begin)) {
			return false
		}
		endIndex := bytes.Index(data, []byte(end))
		if endIndex < 0 {
			return false
		}
		endIndex += len(end)
		// pem.Decode skips malformed prefixes, so isolate one block and reject
		// another header before decoding it.
		if bytes.Contains(data[len(begin):endIndex], []byte("-----BEGIN")) {
			return false
		}
		block, rest := pem.Decode(data[:endIndex])
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
			return false
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return false
		}
		count++
		data = data[endIndex:]
	}
	return count > 0
}

// ModelVersionResolver reports the live search model version for a tenant.
type ModelVersionResolver = search.ModelVersionResolver

// Adapter implements the search query and projection ports using OpenSearch.
type Adapter struct {
	*search.Adapter
	modelVersion ModelVersionResolver
	client       *native.Client
	closed       atomic.Bool
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

// Open connects with the native OpenSearch client and verifies the server's
// distribution identity before exposing the search port.
func Open(ctx context.Context, cfg Config, reg *registry.Registry, opts ...Option) (*Adapter, error) {
	if len(cfg.Addrs) == 0 {
		return nil, safeError("opensearch addresses", fabriqerr.CodeInvalidInput)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ca := cfg.CACert
	if len(ca) == 0 {
		ca = nil
	}
	if cfg.CAFile != "" {
		var err error
		ca, err = os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, safeError("opensearch CA file", fabriqerr.CodeInvalidInput)
		}
	}
	discover := false
	clientCfg := native.Config{Addresses: cfg.Addrs, Username: cfg.Username, Password: cfg.Password, CACert: ca, DiscoverNodesOnStart: &discover}
	// Identity checks run on each configured endpoint, so a mixed cluster list
	// cannot pass verification by selecting only its first native node.
	for _, addr := range cfg.Addrs {
		probeCfg := clientCfg
		probeCfg.Addresses = []string{addr}
		probeCfg.DisableRetry = true
		probe, perr := native.NewClient(probeCfg)
		if perr != nil {
			return nil, safeError("opensearch client", fabriqerr.CodeInvalidInput)
		}
		identityErr := verifyIdentity(ctx, probe)
		_ = probe.Close()
		if identityErr != nil {
			return nil, identityErr
		}
	}
	client, err := native.NewClient(clientCfg)
	if err != nil {
		return nil, safeError("opensearch client", fabriqerr.CodeInvalidInput)
	}
	a := &Adapter{client: client}
	for _, opt := range opts {
		opt(a)
	}
	a.Adapter = search.New(nativeTransport{Client: client, closed: &a.closed}, reg, a.modelVersion, translateTransport, translateResponse)
	return a, nil
}

func verifyIdentity(ctx context.Context, client *native.Client) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", http.NoBody)
	if err != nil {
		return translateTransport("opensearch identity", err)
	}
	res, err := client.Stream(req)
	if err != nil {
		return translateTransport("opensearch identity", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, res.Body); _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		return translateResponse("opensearch identity", res.StatusCode, "")
	}
	var info struct {
		Version struct {
			Distribution string `json:"distribution"`
		} `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&info); err != nil || info.Version.Distribution != "opensearch" {
		return safeError("opensearch identity", fabriqerr.CodeInvalidInput)
	}
	return nil
}

func safeError(op string, code fabriqerr.Code) error {
	return fabriqerr.New(code, fabriqerr.SafeMessage(code), fabriqerr.WithOp(op), fabriqerr.WithRetryable(code == fabriqerr.CodeUnavailable), fabriqerr.WithMeta(fabriqerr.Meta{Driver: "opensearch"}))
}
func translateTransport(op string, err error) error {
	code := fabriqerr.CodeUnavailable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = fabriqerr.CodeTimeout
	}
	return safeError(op, code)
}
func translateResponse(op string, status int, _ string) error {
	code := fabriqerr.CodeInternal
	switch status {
	case 400:
		code = fabriqerr.CodeInvalidInput
	case 401:
		code = fabriqerr.CodeUnauthorized
	case 403:
		code = fabriqerr.CodePermissionDenied
	case 404:
		code = fabriqerr.CodeNotFound
	case 409:
		code = fabriqerr.CodeVersionConflict
	case 429:
		code = fabriqerr.CodeUnavailable
	default:
		if status >= 500 {
			code = fabriqerr.CodeUnavailable
		}
	}
	return safeError(op, code)
}

// SearchTargetName names the search projection's versioned rebuild target.
func SearchTargetName(tenantID string, version int) string {
	return search.TargetName(tenantID, version)
}

// nativeTransport keeps the native client's streaming response ownership explicit.
type nativeTransport struct {
	*native.Client
	closed *atomic.Bool
}

func (t nativeTransport) Perform(req *http.Request) (*http.Response, error) {
	if t.closed.Load() {
		return nil, errors.New("opensearch adapter closed")
	}
	return t.Stream(req)
}

// Close stops background transport work and releases idle connections.
func (a *Adapter) Close() error {
	a.closed.Store(true)
	if err := a.client.Close(); err != nil {
		return safeError("opensearch close", fabriqerr.CodeInternal)
	}
	return nil
}
