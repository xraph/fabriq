package redis_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/internal/enginetest"
)

// Managed Redis and Valkey (DigitalOcean, ElastiCache with in-transit
// encryption, Upstash) only accept TLS, so the stream transport has to speak
// it, with the certificate and the host checked.
func TestRedisTransport(t *testing.T) {
	for _, tc := range []struct {
		name, mode, username, certHost      string
		tls, password, wrongRoot, wrongHost bool
	}{
		{name: "default plaintext"},
		{name: "disabled TLS with default user", mode: "disable", password: true},
		{name: "named ACL over plaintext", username: "stream-worker", password: true},
		{name: "named ACL with verified IP", mode: "verify-full", username: "stream-worker", tls: true, password: true, certHost: "127.0.0.1"},
		{name: "system roots by default", mode: "verify-full", tls: true, certHost: "127.0.0.1", wrongRoot: true},
		{name: "verified DNS hostname", mode: "verify-full", tls: true, certHost: "localhost"},
		{name: "wrong host", mode: "verify-full", tls: true, certHost: "valkey.invalid", wrongHost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var serverTLS *tls.Config
			var ca []byte
			if tc.tls {
				serverTLS, ca = enginetest.TLS(t, tc.certHost)
			}
			if tc.wrongRoot {
				// The server's CA is not given, so only the system pool is left.
				ca = nil
			}
			password := ""
			if tc.password {
				password = "fixture-password"
			}
			endpoint := enginetest.StartRESP(t, serverTLS, tc.username, password)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			addr := endpoint.Addr
			if tc.certHost == "localhost" {
				_, port, err := net.SplitHostPort(addr)
				if err != nil {
					t.Fatal(err)
				}
				addr = net.JoinHostPort("localhost", port)
			}
			a, err := redis.Open(ctx, redisConfig(t, addr, tc.username, password, tc.mode, ca))
			if a != nil {
				defer func() { _ = a.Close() }()
			}
			switch {
			case tc.wrongRoot:
				// Linux reports x509.UnknownAuthorityError and macOS its own
				// verifier's error. Both arrive wrapped in this one.
				var trustErr *tls.CertificateVerificationError
				if !errors.As(err, &trustErr) {
					t.Fatalf("Open without the server's CA = %v", err)
				}
			case tc.wrongHost:
				var hostnameErr x509.HostnameError
				if !errors.As(err, &hostnameErr) {
					t.Fatalf("Open with wrong host = %v", err)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Ping(ctx); err != nil {
					t.Fatalf("Ping = %v", err)
				}
				if endpoint.Pings.Load() != 2 {
					t.Fatal("Open and Ping did not both reach an authenticated PING")
				}
			}
		})
	}
}

func TestRedisRejectsTrustBeforeDial(t *testing.T) {
	_, ca := enginetest.TLS(t, "127.0.0.1")
	endpoint := enginetest.StartRESP(t, nil, "", "fixture-password")
	for _, tc := range []struct {
		name, addr, mode string
		ca               []byte
	}{
		{name: "unknown mode", addr: endpoint.Addr, mode: "secret-mode"},
		{name: "CA with implicit plaintext", addr: endpoint.Addr, ca: ca},
		{name: "CA with disabled TLS", addr: endpoint.Addr, mode: "disable", ca: ca},
		{name: "URL in socket address", addr: "rediss://" + endpoint.Addr, mode: "verify-full", ca: ca},
		{name: "empty host", addr: ":6379", mode: "verify-full", ca: ca},
		{name: "invalid port", addr: "localhost:65536", mode: "verify-full", ca: ca},
		{name: "not a certificate", addr: endpoint.Addr, mode: "verify-full", ca: []byte("-----BEGIN CERTIFICATE-----\nbroken\n")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			a, err := redis.Open(ctx, redisConfig(t, tc.addr, "", "fixture-password", tc.mode, tc.ca))
			if a != nil {
				_ = a.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "redis") || strings.Contains(err.Error(), "ping") {
				t.Fatalf("expected a Redis config error, got %v", err)
			}
			for _, forbidden := range []string{tc.addr, "fixture-password", "secret-mode", "BEGIN CERTIFICATE"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatal("public error exposed a config value")
				}
			}
			if endpoint.Dials.Load() != 0 {
				t.Fatal("invalid trust settings reached the socket")
			}
		})
	}
}

func redisConfig(t *testing.T, addr, username, password, mode string, ca []byte) redis.Config {
	t.Helper()
	// JSON keeps these tests compiling against a Config without the trust
	// fields, so they fail on behavior rather than on the build.
	var cfg redis.Config
	raw, err := json.Marshal(map[string]any{"Addr": addr, "Username": username, "Password": password, "TLSMode": mode, "CACert": ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}
