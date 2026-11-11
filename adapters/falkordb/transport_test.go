package falkordb_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/core/registry"
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
			addr := endpoint.addr
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
				if endpoint.pings.Load() != 2 {
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
		{name: "unknown mode", addr: endpoint.addr, mode: "secret-mode"},
		{name: "CA with implicit plaintext", addr: endpoint.addr, ca: ca},
		{name: "CA with disabled TLS", addr: endpoint.addr, mode: "disable", ca: ca},
		{name: "URL in socket address", addr: "rediss://" + endpoint.addr, mode: "verify-full", ca: ca},
		{name: "empty host", addr: ":6379", mode: "verify-full", ca: ca},
		{name: "invalid port", addr: "localhost:65536", mode: "verify-full", ca: ca},
		{name: "malformed skipped prefix", addr: endpoint.addr, mode: "verify-full", ca: append([]byte("-----BEGIN CERTIFICATE-----\nbroken\n"), ca...)},
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
			if endpoint.dials.Load() != 0 {
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
			a, err := falkordb.Open(ctx, falkorConfig(t, endpoint.addr, "worker", "fixture-password", "verify-full", ca), registry.New(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = a.Close() })
			endpoint.stall.Store(true)

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
			case <-endpoint.handshake:
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
			case <-endpoint.socketClosed:
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

type falkorFixture struct {
	addr                    string
	dials, pings            atomic.Int32
	stall                   atomic.Bool
	handshake, socketClosed chan struct{}
}

func startFalkorFixture(t *testing.T, certificate *tls.Config, username, password string) *falkorFixture {
	t.Helper()
	f := &falkorFixture{handshake: make(chan struct{}, 1), socketClosed: make(chan struct{}, 1)}
	f.addr = startEngineListener(t, func(conn net.Conn) {
		f.dials.Add(1)
		if f.stall.Load() {
			var recordType [1]byte
			if _, err := io.ReadFull(conn, recordType[:]); err != nil || recordType[0] != 22 {
				return
			}
			f.handshake <- struct{}{}
			_, _ = io.Copy(io.Discard, conn)
			f.socketClosed <- struct{}{}
			return
		}
		if certificate != nil {
			conn = tls.Server(conn, certificate)
		}
		f.serve(conn, username, password)
	})
	return f
}

func startEngineListener(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				defer stop()
				serve(conn)
			})
		}
	})
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		workers.Wait()
	})
	return listener.Addr().String()
}

func (f *falkorFixture) serve(conn net.Conn, username, password string) {
	reader := bufio.NewReader(conn)
	authenticated := password == ""
	for {
		args := readRESPCommand(reader)
		if len(args) == 0 {
			return
		}
		var reply string
		switch strings.ToLower(args[0]) {
		case "hello":
			// Use RESP2 fallback so AUTH and the selected ACL user are observable.
			reply = "-ERR unknown command\r\n"
		case "auth":
			if username == "" {
				authenticated = len(args) == 2 && args[1] == password
			} else {
				authenticated = len(args) == 3 && args[1] == username && args[2] == password
			}
			reply = "-WRONGPASS invalid credentials\r\n"
			if authenticated {
				reply = "+OK\r\n"
			}
		case "client":
			reply = "+OK\r\n"
		case "ping":
			reply = "-NOAUTH Authentication required\r\n"
			if authenticated {
				f.pings.Add(1)
				reply = "+PONG\r\n"
			}
		default:
			reply = "-ERR unknown command\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func readRESPCommand(reader *bufio.Reader) []string {
	readLength := func(marker byte) (int, bool) {
		line, err := reader.ReadString('\n')
		if err != nil || len(line) < 4 || line[0] != marker || !strings.HasSuffix(line, "\r\n") {
			return 0, false
		}
		n, err := strconv.Atoi(line[1 : len(line)-2])
		return n, err == nil && n >= 0 && n <= 1024
	}
	count, ok := readLength('*')
	if !ok {
		return nil
	}
	args := make([]string, 0, count)
	for range count {
		size, ok := readLength('$')
		if !ok {
			return nil
		}
		arg := make([]byte, size+2)
		if _, err := io.ReadFull(reader, arg); err != nil || string(arg[size:]) != "\r\n" {
			return nil
		}
		args = append(args, string(arg[:size]))
	}
	return args
}

func falkorTLS(t *testing.T, host string) (*tls.Config, []byte) {
	t.Helper()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	root := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: root.NotBefore, NotAfter: root.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(host); ip != nil {
		leaf.IPAddresses = []net.IP{ip}
	} else {
		leaf.DNSNames = []string{host}
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, &leafKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}}, MinVersion: tls.VersionTLS12}, ca
}
