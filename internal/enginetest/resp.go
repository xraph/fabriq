// Package enginetest serves fake projection and transport engines for
// adapter tests: a RESP endpoint (Redis, FalkorDB) with optional TLS, and the
// certificates to verify it.
package enginetest

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// RESPServer is a RESP endpoint that answers HELLO, AUTH, CLIENT and PING
// the way Redis and FalkorDB do, optionally behind TLS. Dials counts accepted
// sockets and Pings authenticated PINGs. With Stall set, a socket that starts
// a TLS handshake is held open: Handshake fires and the server reads until
// the client gives up, then SocketClosed fires.
type RESPServer struct {
	Addr                    string
	Dials, Pings            atomic.Int32
	Stall                   atomic.Bool
	Handshake, SocketClosed chan struct{}
}

// StartRESP serves a RESPServer on a loopback port until the test ends. A
// nil certificate serves plaintext; an empty password needs no AUTH.
func StartRESP(t *testing.T, certificate *tls.Config, username, password string) *RESPServer {
	t.Helper()
	f := &RESPServer{Handshake: make(chan struct{}, 1), SocketClosed: make(chan struct{}, 1)}
	f.Addr = Listen(t, func(conn net.Conn) {
		f.Dials.Add(1)
		if f.Stall.Load() {
			var recordType [1]byte
			if _, err := io.ReadFull(conn, recordType[:]); err != nil || recordType[0] != 22 {
				return
			}
			f.Handshake <- struct{}{}
			_, _ = io.Copy(io.Discard, conn)
			f.SocketClosed <- struct{}{}
			return
		}
		if certificate != nil {
			conn = tls.Server(conn, certificate)
		}
		f.serve(conn, username, password)
	})
	return f
}

// Listen accepts loopback connections and hands each to serve until the
// test ends. It returns the listening address.
func Listen(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	var lc net.ListenConfig
	listener, err := lc.Listen(t.Context(), "tcp4", "127.0.0.1:0")
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

func (f *RESPServer) serve(conn net.Conn, username, password string) {
	reader := bufio.NewReader(conn)
	authenticated := password == ""
	for {
		args := ReadCommand(reader)
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
				f.Pings.Add(1)
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

// ReadCommand reads one RESP array of bulk strings, or nil on anything else.
func ReadCommand(reader *bufio.Reader) []string {
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

// TLS makes a fresh CA and a server certificate for host (an IP or a DNS
// name). It returns the server config and the CA as PEM.
func TLS(t *testing.T, host string) (server *tls.Config, caPEM []byte) {
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
