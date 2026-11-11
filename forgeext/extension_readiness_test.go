package forgeext

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/vessel"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
)

const readinessProbeSecret = "readiness-probe-credential"

func TestExtensionReadyOptionalDependencies(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		ext := startReadinessExtension(t, fabriq.Config{})
		assertExtensionReadiness(t, ext, "")
	})

	t.Run("falkordb", func(t *testing.T) {
		var unavailable atomic.Bool
		addr := listenReadinessBackend(t, func(conn net.Conn) {
			serveReadinessFalkor(conn, func(_ *bufio.Reader) string {
				if unavailable.Load() {
					return "-ERR " + readinessProbeSecret + "\r\n"
				}
				return "+PONG\r\n"
			})
		})
		ext := startReadinessExtension(t, fabriq.Config{FalkorDB: fabriq.FalkorDBConfig{Addr: addr}})
		assertExtensionReadiness(t, ext, "")

		unavailable.Store(true)
		assertExtensionReadiness(t, ext, "fabriq: falkordb is not reachable")
	})

	for _, engine := range []string{"elasticsearch", "opensearch"} {
		t.Run(engine, func(t *testing.T) {
			var unavailable atomic.Bool
			cfg := readinessSearchConfig(t, engine, func(w http.ResponseWriter, _ *http.Request) bool {
				if !unavailable.Load() {
					return false
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"error":{"type":"unavailable","reason":"`+readinessProbeSecret+`"}}`)
				return true
			})
			ext := startReadinessExtension(t, cfg)
			assertExtensionReadiness(t, ext, "")

			unavailable.Store(true)
			assertExtensionReadiness(t, ext, "fabriq: search is not reachable")
			if engine == "elasticsearch" {
				assertSafeSearchReadinessError(t, ext.Ready(t.Context()), fabriqerr.CodeUnavailable)
			}
		})
	}
}

func TestExtensionReadyFalkorInterruptsProbe(t *testing.T) {
	for _, mode := range []string{"deadline", "cancellation", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			var checking atomic.Bool
			pingReceived := make(chan net.Conn, 1)
			peerClosed := make(chan error, 1)
			addr := listenReadinessBackend(t, func(conn net.Conn) {
				serveReadinessFalkor(conn, func(reader *bufio.Reader) string {
					if !checking.Load() {
						return "+PONG\r\n"
					}
					pingReceived <- conn
					_, err := reader.ReadByte()
					peerClosed <- err
					return ""
				})
			})
			ext := startReadinessExtension(t, fabriq.Config{FalkorDB: fabriq.FalkorDBConfig{Addr: addr}})
			checking.Store(true)

			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(t.Context(), time.Second)
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			result := make(chan error, 1)
			var worker sync.WaitGroup
			worker.Go(func() { result <- ext.Ready(ctx) })
			t.Cleanup(worker.Wait)

			var conn net.Conn
			select {
			case conn = <-pingReceived:
			case <-time.After(2 * time.Second):
				t.Fatal("FalkorDB did not receive the readiness PING")
			}
			defer conn.Close()

			switch mode {
			case "deadline":
				<-ctx.Done()
			case "cancellation":
				cancel()
			case "shutdown":
				stopped := make(chan error, 1)
				worker.Go(func() { stopped <- ext.Stop(t.Context()) })
				select {
				case err := <-stopped:
					if err != nil {
						t.Fatalf("Stop: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Stop still waiting on the FalkorDB readiness socket")
				}
			}
			select {
			case err := <-result:
				if err == nil || err.Error() != "fabriq: falkordb is not reachable" || errors.Unwrap(err) != nil {
					t.Fatalf("Ready after FalkorDB %s = %v, want a safe graph error", mode, err)
				}
			case <-time.After(time.Second):
				_, _ = io.WriteString(conn, "+PONG\r\n")
				t.Fatalf("Ready still waiting for FalkorDB after %s", mode)
			}
			select {
			case err := <-peerClosed:
				if !errors.Is(err, io.EOF) {
					t.Fatalf("FalkorDB readiness socket read = %v, want EOF", err)
				}
			case <-time.After(time.Second):
				t.Fatal("FalkorDB readiness socket is still open")
			}

			if mode == "shutdown" {
				return
			}
			checking.Store(false)
			if err := ext.stores.Falkor.Ping(t.Context()); err != nil {
				t.Fatalf("shared FalkorDB client after readiness %s: %v", mode, err)
			}
		})
	}
}

func TestExtensionReadyFalkorAfterStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reg := registry.New()
	owner, err := postgres.Open(ctx, readinessPostgresDSN(t), reg)
	if err != nil {
		t.Fatalf("open host Postgres: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	var connections atomic.Int64
	addr := listenReadinessBackend(t, func(conn net.Conn) {
		connections.Add(1)
		serveReadinessFalkor(conn, func(_ *bufio.Reader) string { return "+PONG\r\n" })
	})
	app := forge.New(forge.WithAppName("fabriq-readiness"), forge.WithAppLogger(forge.NewNoopLogger()))
	if err := vessel.Provide(app.Container(), func() (*grove.DB, error) { return owner.Grove(), nil }); err != nil {
		t.Fatalf("register host Grove: %v", err)
	}
	ext := New(reg, WithConfig(fabriq.Config{FalkorDB: fabriq.FalkorDBConfig{Addr: addr}}))
	if err := ext.BaseExtension.Register(app); err != nil {
		t.Fatalf("register extension with host: %v", err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if ext.IsStarted() {
			_ = ext.Stop(context.Background())
		}
	})
	if err := ext.Ready(ctx); err != nil {
		t.Fatalf("Ready before Stop: %v", err)
	}
	graph := ext.stores.Falkor
	if err := ext.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := owner.Ping(ctx); err != nil {
		t.Fatalf("host Postgres after Stop: %v", err)
	}

	before := connections.Load()
	if err := graph.PingReadiness(ctx); err == nil {
		t.Error("closed FalkorDB adapter accepted a readiness ping")
	}
	if err := ext.Ready(ctx); err == nil {
		t.Error("Ready reported ready after Stop with borrowed Postgres")
	}
	if after := connections.Load(); after != before {
		t.Errorf("readiness opened %d FalkorDB connections after Stop", after-before)
	}
}

func TestExtensionReadySearchHonorsCancellation(t *testing.T) {
	for _, engine := range []string{"opensearch", "elasticsearch"} {
		t.Run(engine, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var checking atomic.Bool
			cfg := readinessSearchConfig(t, engine, func(w http.ResponseWriter, r *http.Request) bool {
				if !checking.Load() {
					return false
				}
				cancel()
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
					w.WriteHeader(http.StatusBadRequest)
				}
				return true
			})
			ext := startReadinessExtension(t, cfg)
			checking.Store(true)

			err := ext.Ready(ctx)
			if engine == "elasticsearch" {
				assertSafeSearchReadinessError(t, err, fabriqerr.CodeTimeout)
			} else if fabriqerr.CodeOf(err) != fabriqerr.CodeTimeout {
				t.Fatalf("Ready after caller cancellation during search = %v, want timeout", err)
			}
		})
	}
}

func assertSafeSearchReadinessError(t *testing.T, err error, code fabriqerr.Code) {
	t.Helper()
	var probeErr *fabriqerr.Error
	if !errors.As(err, &probeErr) || probeErr.Code != code {
		t.Fatalf("Ready search error = %v, want code %q", err, code)
	}
	if strings.Contains(err.Error(), readinessProbeSecret) {
		t.Error("Ready search error text exposed a backend credential")
	}
	if probeErr.Unwrap() != nil {
		t.Error("Ready search error chain retains a backend cause")
	}
	if !reflect.DeepEqual(probeErr.Meta, fabriqerr.Meta{}) {
		t.Error("Ready search error retains backend metadata")
	}
}

func assertExtensionReadiness(t *testing.T, ext *Extension, reasonPrefix string) {
	t.Helper()
	w, body := probeGet(t, "/_/readyz", ext.Ready)
	if strings.Contains(w.Body.String(), readinessProbeSecret) {
		t.Fatal("readiness response exposed a backend credential")
	}
	if reasonPrefix == "" {
		if w.Code != http.StatusOK || body["status"] != "ready" {
			t.Fatalf("readyz = %d %v, want ready", w.Code, body)
		}
		return
	}
	if w.Code != http.StatusServiceUnavailable || body["status"] != "not ready" ||
		!strings.HasPrefix(body["reason"], reasonPrefix) {
		t.Fatalf("readyz = %d %v, want 503 with reason %q", w.Code, body, reasonPrefix)
	}
}

func startReadinessExtension(t *testing.T, cfg fabriq.Config) *Extension {
	t.Helper()
	cfg.Postgres.DSN = readinessPostgresDSN(t)
	ext := New(registry.New(), WithConfig(cfg))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if ext.IsStarted() {
			_ = ext.Stop(context.Background())
		}
	})
	return ext
}

func readinessPostgresDSN(t *testing.T) string {
	t.Helper()
	addr := listenReadinessBackend(t, serveReadinessPostgres)
	return "postgres://readiness@" + addr + "/readiness?sslmode=disable&default_query_exec_mode=simple_protocol"
}

func readinessSearchConfig(t *testing.T, engine string, respond func(http.ResponseWriter, *http.Request) bool) fabriq.Config {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if engine == "elasticsearch" {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
		}
		w.Header().Set("Content-Type", "application/json")
		if respond(w, r) {
			return
		}
		if engine == "elasticsearch" {
			_, _ = io.WriteString(w, `{"version":{"number":"9.4.1"}}`)
		} else {
			_, _ = io.WriteString(w, `{"version":{"distribution":"opensearch","number":"3.9.0"}}`)
		}
	}))
	t.Cleanup(srv.Close)
	if engine == "elasticsearch" {
		return fabriq.Config{Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{srv.URL}}}
	}
	return fabriq.Config{OpenSearch: fabriq.OpenSearchConfig{Addrs: []string{srv.URL}}}
}

func listenReadinessBackend(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	ctx := t.Context()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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
		_ = listener.Close()
		workers.Wait()
	})
	return listener.Addr().String()
}

// serveReadinessPostgres supports the source adapter's startup and SELECT 1.
func serveReadinessPostgres(conn net.Conn) {
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
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
			return
		}
		switch query.String {
		case "SELECT 1":
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{Name: []byte("one"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1},
			}})
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case "-- ping":
			backend.Send(&pgproto3.EmptyQueryResponse{})
		default:
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: "unexpected readiness query"})
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return
		}
	}
}

// serveReadinessFalkor answers the client's startup commands and PING.
func serveReadinessFalkor(conn net.Conn, ping func(*bufio.Reader) string) {
	reader := bufio.NewReader(conn)
	for {
		header, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		var args int
		if _, err := fmt.Sscanf(header, "*%d", &args); err != nil || args < 1 {
			return
		}
		var command string
		for i := range args {
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			arg, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if i == 0 {
				command = strings.ToUpper(strings.TrimSuffix(arg, "\r\n"))
			}
		}
		response := "-ERR unknown command\r\n"
		switch command {
		case "CLIENT":
			response = "+OK\r\n"
		case "PING":
			response = ping(reader)
			if response == "" {
				return
			}
		}
		if _, err := io.WriteString(conn, response); err != nil {
			return
		}
	}
}
