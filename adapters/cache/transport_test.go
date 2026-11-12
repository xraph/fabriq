package cache_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/cache"
	"github.com/xraph/fabriq/internal/enginetest"
)

// The cache shares the stream transport's Redis, so it has to reach it the
// same way: over verified TLS when the server wants it.
func TestCacheTransport(t *testing.T) {
	serverTLS, ca := enginetest.TLS(t, "127.0.0.1")
	for _, tc := range []struct {
		name, mode string
		tls        bool
		ca         []byte
	}{
		{name: "plaintext"},
		{name: "verified TLS", mode: "verify-full", tls: true, ca: ca},
		{name: "unknown CA", mode: "verify-full", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var certificate *tls.Config
			if tc.tls {
				certificate = serverTLS
			}
			// A password with URL syntax in it has to survive the trip too.
			const password = "p@ss/word:1"
			endpoint := enginetest.StartRESP(t, certificate, "cache-worker", password)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			a, err := cache.Open(ctx, cacheConfig(t, endpoint.Addr, "cache-worker", password, tc.mode, tc.ca))
			if a != nil {
				defer func() { _ = a.Close() }()
			}
			if tc.tls && tc.ca == nil {
				var verifyErr *tls.CertificateVerificationError
				if !errors.As(err, &verifyErr) {
					t.Fatalf("Open without the server's CA = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if endpoint.Pings.Load() == 0 {
				t.Fatal("Open never reached an authenticated PING")
			}
		})
	}
}

func TestCacheRejectsTrustBeforeDial(t *testing.T) {
	_, ca := enginetest.TLS(t, "127.0.0.1")
	endpoint := enginetest.StartRESP(t, nil, "", "")
	for _, tc := range []struct {
		name, mode string
		ca         []byte
	}{
		{name: "unknown mode", mode: "secret-mode"},
		{name: "CA over plaintext", ca: ca},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			a, err := cache.Open(ctx, cacheConfig(t, endpoint.Addr, "", "", tc.mode, tc.ca))
			if a != nil {
				_ = a.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "redis") {
				t.Fatalf("expected a Redis config error, got %v", err)
			}
			if endpoint.Dials.Load() != 0 {
				t.Fatal("invalid trust settings reached the socket")
			}
		})
	}
}

func cacheConfig(t *testing.T, addr, username, password, mode string, ca []byte) cache.Config {
	t.Helper()
	var cfg cache.Config
	raw, err := json.Marshal(map[string]any{"Addr": addr, "Username": username, "Password": password, "TLSMode": mode, "CACert": ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
