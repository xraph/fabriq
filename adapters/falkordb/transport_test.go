package falkordb_test

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

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/internal/enginetest"
)

func TestFalkorTransport(t *testing.T) {
	for _, tc := range []struct {
		name, mode, username, certHost      string
		tls, password, wrongRoot, wrongHost bool
	}{
		{name: "default plaintext"},
		{name: "disabled TLS with default user", mode: "disable", password: true},
		{name: "named ACL over plaintext", username: "graph-worker", password: true},
		{name: "named ACL with verified IP", mode: "verify-full", username: "graph-worker", tls: true, password: true, certHost: "127.0.0.1"},
		{name: "wrong trust root", mode: "verify-full", tls: true, certHost: "127.0.0.1", wrongRoot: true},
		{name: "verified DNS hostname", mode: "verify-full", tls: true, certHost: "localhost"},
		{name: "wrong host", mode: "verify-full", tls: true, certHost: "falkor.invalid", wrongHost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var serverTLS *tls.Config
			var ca []byte
			if tc.tls {
				serverTLS, ca = falkorTLS(t, tc.certHost)
			}
			if tc.wrongRoot {
				_, ca = falkorTLS(t, tc.certHost)
			}
			password := ""
			if tc.password {
				password = "fixture-password"
			}
			endpoint := startFalkorFixture(t, serverTLS, tc.username, password)
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
			a, err := falkordb.Open(ctx, falkorConfig(t, addr, tc.username, password, tc.mode, ca), registry.New(), nil)
			if a != nil {
				defer func() { _ = a.Close() }()
			}
			switch {
			case tc.wrongRoot:
				var trustErr x509.UnknownAuthorityError
				if !errors.As(err, &trustErr) {
					t.Fatalf("Open with wrong root = %v", err)
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
				if err := a.PingReadiness(ctx); err != nil {
					t.Fatalf("readiness = %v", err)
				}
				if endpoint.Pings.Load() != 2 {
					t.Fatal("Open and readiness did not both reach authenticated PING")
				}
				if err := a.Ping(ctx); err != nil {
					t.Fatalf("shared pool after readiness = %v", err)
				}
			}
		})
	}
}

func TestFalkorRejectsTrustBeforeDial(t *testing.T) {
	_, ca := falkorTLS(t, "127.0.0.1")
	endpoint := startFalkorFixture(t, nil, "", "fixture-password")
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
		{name: "malformed skipped prefix", addr: endpoint.Addr, mode: "verify-full", ca: append([]byte("-----BEGIN CERTIFICATE-----\nbroken\n"), ca...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			a, err := falkordb.Open(ctx, falkorConfig(t, tc.addr, "", "fixture-password", tc.mode, tc.ca), registry.New(), nil)
			if a != nil {
				_ = a.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "falkordb") || strings.Contains(err.Error(), "ping") {
				t.Fatalf("expected FalkorDB config error, got %v", err)
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

func TestFalkorReadinessCancelsStalledTLS(t *testing.T) {
	for _, action := range []string{"deadline", "cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			certificate, ca := falkorTLS(t, "127.0.0.1")
			endpoint := startFalkorFixture(t, certificate, "worker", "fixture-password")
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			a, err := falkordb.Open(ctx, falkorConfig(t, endpoint.Addr, "worker", "fixture-password", "verify-full", ca), registry.New(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })
			endpoint.Stall.Store(true)

			probeCtx, stopProbe := context.WithCancel(ctx)
			defer stopProbe()
			if action == "deadline" {
				var stopDeadline context.CancelFunc
				probeCtx, stopDeadline = context.WithTimeout(probeCtx, 500*time.Millisecond)
				defer stopDeadline()
			}
			probe := make(chan error, 1)
			go func() { probe <- a.PingReadiness(probeCtx) }()
			select {
			case <-endpoint.Handshake:
			case <-ctx.Done():
				t.Fatal("readiness did not start its TLS handshake")
			}

			want := context.DeadlineExceeded
			switch action {
			case "cancel":
				want = context.Canceled
				stopProbe()
			case "close":
				want = redis.ErrClosed
				closed := make(chan error, 1)
				go func() { closed <- a.Close() }()
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("Close = %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Close waited for the stalled handshake")
				}
			}
			select {
			case err := <-probe:
				if !errors.Is(err, want) {
					t.Fatalf("readiness = %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("readiness waited for the stalled handshake")
			}
			select {
			case <-endpoint.SocketClosed:
			case <-time.After(time.Second):
				t.Fatal("stalled handshake socket remained open")
			}
			if action != "close" {
				if err := a.Ping(ctx); err != nil {
					t.Fatalf("shared pool after readiness cancellation = %v", err)
				}
			}
		})
	}
}

func falkorConfig(t *testing.T, addr, username, password, mode string, ca []byte) falkordb.Config {
	t.Helper()
	// JSON keeps the same behavioral tests runnable against the baseline, where
	// the adapter ignores the two new trust fields.
	var cfg falkordb.Config
	raw, err := json.Marshal(map[string]any{"Addr": addr, "Username": username, "Password": password, "TLSMode": mode, "CACert": ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func startFalkorFixture(t *testing.T, certificate *tls.Config, username, password string) *enginetest.RESPServer {
	t.Helper()
	return enginetest.StartRESP(t, certificate, username, password)
}

func falkorTLS(t *testing.T, host string) (*tls.Config, []byte) {
	t.Helper()
	return enginetest.TLS(t, host)
}
