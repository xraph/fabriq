// Command fabriqserver is a REFERENCE fabriq-as-a-server binary (ADR 0009): it
// opens an embedded fabriq (owning its datastore pools) and serves it over gRPC,
// so remote clients dial it with fabriq+grpc://<tenant>@host and hold the same
// query.Fabric they would embed.
//
// It lives in its own leaf module on purpose — a server needs BOTH the full
// engine and google.golang.org/grpc, and keeping that combined dependency graph
// in a leaf keeps it out of the core module and the lean remote/grpc binding.
//
// This is a template, not a turnkey deployment. Three things are yours to own:
// the entity REGISTRY (here: the demo domain), the CONFIG source (here: env),
// and the AUTHENTICATOR (here: a DEV bearer-token-as-tenant shim — see auth.go;
// replace it with real credential verification before production).
//
// Environment:
//
//	FABRIQ_POSTGRES_DSN    (required)
//	FABRIQ_REDIS_ADDR      (optional; enables the live/subscribe plane)
//	FABRIQ_GRPC_ADDR       (default :7000)
//	FABRIQ_TLS_CERT        (optional; PEM server cert — enables TLS)
//	FABRIQ_TLS_KEY         (optional; PEM server key)
//	FABRIQ_TLS_CLIENT_CA   (optional; PEM CA — enables mTLS: RequireAndVerifyClientCert)
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	remotegrpc "github.com/xraph/fabriq/remote/grpc"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/remote"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("fabriqserver: %v", err)
	}
}

func run() error {
	dsn := os.Getenv("FABRIQ_POSTGRES_DSN")
	if dsn == "" {
		return errors.New("FABRIQ_POSTGRES_DSN is required")
	}
	addr := os.Getenv("FABRIQ_GRPC_ADDR")
	if addr == "" {
		addr = ":7000"
	}

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		return fmt.Errorf("register entities: %w", err)
	}

	ctx := context.Background()
	f, _, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: dsn},
		Redis:    fabriq.RedisConfig{Addr: os.Getenv("FABRIQ_REDIS_ADDR")},
	})
	if err != nil {
		return fmt.Errorf("open fabriq: %w", err)
	}
	defer func() { _ = f.Close() }()

	tlsCfg, err := serverTLS()
	if err != nil {
		return fmt.Errorf("tls config: %w", err)
	}
	var opts []remotegrpc.ServerOption
	if tlsCfg != nil {
		opts = append(opts, remotegrpc.WithServerTLS(tlsCfg))
	}

	srv := remotegrpc.NewServer(remote.NewHandler(f, reg), devAuthenticator(), opts...)

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Printf("fabriqserver: serving fabriq over gRPC on %s (tls=%t)", lis.Addr(), tlsCfg != nil)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-serveErr:
		return err
	case s := <-sig:
		log.Printf("fabriqserver: %s — draining", s)
		srv.GracefulStop()
		return nil
	}
}

// serverTLS builds the server's TLS config from env, or nil for plaintext
// (dev/localhost only). A client CA turns on mTLS.
func serverTLS() (*tls.Config, error) {
	certFile, keyFile := os.Getenv("FABRIQ_TLS_CERT"), os.Getenv("FABRIQ_TLS_KEY")
	if certFile == "" || keyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}

	if caFile := os.Getenv("FABRIQ_TLS_CLIENT_CA"); caFile != "" {
		caPEM, err := os.ReadFile(caFile) // #nosec G304 G703 -- caFile is an operator-provided config path (env), not attacker input.
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no certificates parsed from %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
