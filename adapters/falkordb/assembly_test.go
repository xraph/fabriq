package falkordb_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
)

func TestFabriqEngineTrustAssembly(t *testing.T) {
	graphTLS, graphCA := falkorTLS(t, "127.0.0.1")
	for _, mode := range []string{"static", "catalog"} {
		t.Run(mode, func(t *testing.T) {
			graph := startFalkorFixture(t, graphTLS, "graph-worker", "fixture-password")
			var searchRequests atomic.Int32
			search := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if user, pass, ok := r.BasicAuth(); !ok || user != "search-worker" || pass != "fixture-password" {
					t.Error("Elasticsearch did not receive the configured Basic auth")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				searchRequests.Add(1)
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				_, _ = io.WriteString(w, `{"version":{"number":"9.4.1"}}`)
			}))
			t.Cleanup(search.Close)
			searchCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: search.Certificate().Raw})
			pg := startEngineListener(t, func(conn net.Conn) { serveCatalogBootstrap(t, conn) })
			dsn := "postgres://fixture@" + pg + "/fixture?sslmode=disable&default_query_exec_mode=simple_protocol"
			cfg := rootEngineConfig(t, map[string]any{
				"falkordb": map[string]any{"addr": graph.addr, "username": "graph-worker", "password": "fixture-password", "tlsMode": "verify-full", "caCert": string(graphCA)},
				"elasticsearch": map[string]any{
					"addrs": []string{search.URL}, "username": "search-worker", "password": "fixture-password", "caCert": string(searchCA),
				},
			})
			if mode == "catalog" {
				cfg.Catalog.DSN = dsn
				cfg.Catalog.ClusterDSNs = map[string]string{"fixture": dsn}
			} else {
				cfg.Postgres.DSN = dsn
			}

			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			fabric, stores, err := fabriq.Open(ctx, registry.New(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stores.Close() }()
			defer func() { _ = fabric.Close() }()
			if err := stores.Falkor.PingReadiness(ctx); err != nil {
				t.Fatalf("graph readiness = %v", err)
			}
			if err := stores.PingSearch(ctx); err != nil {
				t.Fatalf("search readiness = %v", err)
			}
			if graph.pings.Load() != 2 || searchRequests.Load() != 2 {
				t.Fatal("Open and readiness must reach both engines over their configured transports")
			}
		})
	}
}

func TestFabriqEngineTrustValidation(t *testing.T) {
	_, ca := falkorTLS(t, "127.0.0.1")
	for _, tc := range []struct {
		name, engine string
		settings     map[string]any
	}{
		{name: "verified Falkor with no endpoint", engine: "falkordb", settings: map[string]any{"tlsMode": "verify-full"}},
		{name: "explicit plaintext mode with no endpoint", engine: "falkordb", settings: map[string]any{"tlsMode": "disable"}},
		{name: "Falkor CA with no endpoint", engine: "falkordb", settings: map[string]any{"caCert": string(ca)}},
		{name: "Elasticsearch CA with no addresses", engine: "elasticsearch", settings: map[string]any{"caCert": string(ca)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := rootEngineConfig(t, map[string]any{tc.engine: tc.settings})
			cfg.Postgres.DSN = "postgres://fixture/unused"
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.engine) {
				t.Fatalf("Validate = %v, want invalid %s config", err, tc.engine)
			}
		})
	}
}

func rootEngineConfig(t *testing.T, fields map[string]any) fabriq.Config {
	t.Helper()
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	var cfg fabriq.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// serveCatalogBootstrap answers the catalog's startup DDL and cluster role
// check. Tenant databases are lazy and neither engine's ping queries Postgres.
func serveCatalogBootstrap(t *testing.T, conn net.Conn) {
	t.Helper()
	backend := pgproto3.NewBackend(conn, conn)
	start, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := start.(*pgproto3.StartupMessage); !ok {
		t.Error("expected a PostgreSQL startup message")
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}
		query, ok := msg.(*pgproto3.Query)
		if !ok {
			if _, closing := msg.(*pgproto3.Terminate); !closing {
				t.Errorf("unexpected PostgreSQL message %T", msg)
			}
			return
		}
		tag := "CREATE TABLE"
		switch {
		case strings.HasPrefix(query.String, "CREATE TABLE IF NOT EXISTS fabriq_tenant_catalog "):
		case strings.HasPrefix(query.String, "ALTER TABLE fabriq_tenant_catalog "):
			tag = "ALTER TABLE"
		case strings.HasPrefix(query.String, "CREATE INDEX IF NOT EXISTS fabriq_tenant_catalog_state_idx"):
			tag = "CREATE INDEX"
		case query.String == "SELECT current_setting('is_superuser')":
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("current_setting"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("off")}})
			tag = "SELECT 1"
		default:
			t.Errorf("unexpected bootstrap query %q", query.String)
			return
		}
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return
		}
	}
}
