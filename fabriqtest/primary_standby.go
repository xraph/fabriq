//go:build integration

package fabriqtest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xraph/grove/drivers/pgdriver"
)

// WriteProxy is an in-process TCP proxy standing in for a floating write
// endpoint (DNS/VIP/HAProxy). Repoint flips the upstream and force-closes live
// connections so pgx pools reconnect to the new primary — exactly what a
// managed HA failover does under a stable hostname.
type WriteProxy struct {
	ln       net.Listener
	mu       sync.Mutex
	upstream string
	conns    map[net.Conn]struct{}
	closed   bool
}

func newWriteProxy(t *testing.T, upstreamHostPort string) *WriteProxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("write proxy listen: %v", err)
	}
	p := &WriteProxy{ln: ln, upstream: upstreamHostPort, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *WriteProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *WriteProxy) handle(client net.Conn) {
	p.mu.Lock()
	up := p.upstream
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		return
	}
	p.mu.Unlock()

	dialer := net.Dialer{Timeout: 5 * time.Second}
	server, err := dialer.DialContext(context.Background(), "tcp", up)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client)
	p.track(server)
	go func() { _, _ = io.Copy(server, client); _ = server.Close() }()
	_, _ = io.Copy(client, server)
	_ = client.Close()
}

func (p *WriteProxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

// DSN builds a postgres DSN pointed at the proxy from a template DSN (same
// user/db/params, host swapped for the proxy's address).
func (p *WriteProxy) DSN(templateDSN string) string {
	u, _ := url.Parse(templateDSN)
	u.Host = p.ln.Addr().String()
	return u.String()
}

// Repoint flips the upstream and drops every live connection so pools redial.
func (p *WriteProxy) Repoint(upstreamHostPort string) {
	p.mu.Lock()
	p.upstream = upstreamHostPort
	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
	p.mu.Unlock()
}

func (p *WriteProxy) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	_ = p.ln.Close()
}

// UpstreamHostPort extracts host:port from a postgres DSN.
func UpstreamHostPort(dsn string) string {
	u, _ := url.Parse(dsn)
	return u.Host
}

// Promote turns a standby into a primary and waits until it accepts writes.
func Promote(t *testing.T, standbyDSN string) {
	t.Helper()
	ApplyDDL(t, standbyDSN, []string{`SELECT pg_promote()`})
	deadline := time.Now().Add(20 * time.Second)
	for {
		rows := QueryStrings(t, standbyDSN, `SELECT pg_is_in_recovery()::text`)
		if len(rows) == 1 && rows[0] == "false" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("standby never left recovery after pg_promote()")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// StartPrimaryStandby starts a primary (wal streaming enabled) and a hot
// standby streaming from it, plus a WriteProxy in front of the primary.
// Returns the primary DSN, the standby DSN, and the proxy.
func StartPrimaryStandby(t *testing.T) (primaryDSN, standbyDSN string, proxy *WriteProxy) {
	t.Helper()
	primaryDSN, standbyDSN = startReplicatedPair(t)
	proxy = newWriteProxy(t, UpstreamHostPort(primaryDSN))
	return primaryDSN, standbyDSN, proxy
}

// primaryNetworkAlias is the stable in-network hostname the standby uses to
// reach the primary for pg_basebackup and streaming replication.
const primaryNetworkAlias = "pgprimary"

// startReplicatedPair provisions a classic pg_basebackup streaming-replication
// pair on the vanilla postgres:16-alpine image:
//
//   - A shared docker network so the standby can reach the primary by a stable
//     alias (pgprimary) regardless of host-mapped ports.
//   - A PRIMARY tuned for streaming (wal_level=replica, wal senders, hot_standby)
//     with trust auth (test-only) so replication connections need no password —
//     the official image writes a `host replication all all trust` pg_hba line
//     under POSTGRES_HOST_AUTH_METHOD=trust.
//   - A STANDBY whose entrypoint runs pg_basebackup (-R writes standby.signal +
//     primary_conninfo) against the primary before exec'ing postgres, so it comes
//     up as a hot standby streaming from the primary.
//
// It returns host-mapped DSNs for both and only returns once a probe row written
// on the primary is observed on the standby (replication proven).
func startReplicatedPair(t *testing.T) (primaryDSN, standbyDSN string) {
	t.Helper()
	ctx := context.Background()

	net1, err := network.New(ctx)
	if err != nil {
		t.Skipf("fabriqtest: create docker network (no container runtime?): %v", err)
	}
	t.Cleanup(func() {
		if rmErr := net1.Remove(ctx); rmErr != nil {
			t.Logf("fabriqtest: remove network: %v", rmErr)
		}
	})
	netName := net1.Name

	// PRIMARY: streaming-enabled, trust auth for password-free replication.
	//
	// POSTGRES_HOST_AUTH_METHOD=trust makes ordinary connections password-free but
	// does NOT open the separate `replication` pg_hba category, so pg_basebackup
	// from the standby is rejected with "no pg_hba.conf entry for replication
	// connection". An initdb hook explicitly appends a trust rule for replication
	// (test-only) and reloads pg_hba so the running server picks it up.
	replHbaHook := `#!/bin/bash
set -e
{
  echo "host replication all all trust"
  echo "host replication all 0.0.0.0/0 trust"
  echo "host replication all ::/0 trust"
} >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "SELECT pg_reload_conf();"
`
	primary, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          PostgresPlainImage,
			ExposedPorts:   []string{"5432/tcp"},
			Networks:       []string{netName},
			NetworkAliases: map[string][]string{netName: {primaryNetworkAlias}},
			Env: map[string]string{
				"POSTGRES_USER":             "fabriq",
				"POSTGRES_PASSWORD":         "fabriq",
				"POSTGRES_DB":               "fabriq",
				"POSTGRES_HOST_AUTH_METHOD": "trust",
			},
			Files: []testcontainers.ContainerFile{
				{
					Reader:            strings.NewReader(replHbaHook),
					ContainerFilePath: "/docker-entrypoint-initdb.d/00-replication-hba.sh",
					FileMode:          0o755,
				},
			},
			Cmd: []string{
				"postgres",
				"-c", "wal_level=replica",
				"-c", "max_wal_senders=10",
				"-c", "hot_standby=on",
				"-c", "wal_keep_size=64MB",
				"-c", "listen_addresses=*",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("fabriqtest: start primary container (no container runtime?): %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(primary); termErr != nil {
			t.Logf("fabriqtest: terminate primary: %v", termErr)
		}
	})

	// STANDBY: base-backup from the primary over the shared network, then start
	// as a hot standby. The entrypoint override runs the backup first; -R writes
	// standby.signal + primary_conninfo so postgres boots in recovery/streaming.
	standbyScript := fmt.Sprintf(`set -e
rm -rf /var/lib/postgresql/data/*
until pg_basebackup -h %s -p 5432 -U fabriq -D /var/lib/postgresql/data -Fp -Xs -R -P; do
  echo "waiting-for-primary"
  sleep 1
done
chmod 0700 /var/lib/postgresql/data
chown -R postgres:postgres /var/lib/postgresql/data
exec docker-entrypoint.sh postgres`, primaryNetworkAlias)

	standby, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        PostgresPlainImage,
			ExposedPorts: []string{"5432/tcp"},
			Networks:     []string{netName},
			Env: map[string]string{
				// Only consulted if the entrypoint ever fell through to a fresh
				// initdb; the base backup supplies the real data dir.
				"POSTGRES_USER":             "fabriq",
				"POSTGRES_PASSWORD":         "fabriq",
				"POSTGRES_DB":               "fabriq",
				"POSTGRES_HOST_AUTH_METHOD": "trust",
				"PGUSER":                    "fabriq",
			},
			Entrypoint: []string{"bash", "-c"},
			Cmd:        []string{standbyScript},
			WaitingFor: wait.ForLog("database system is ready to accept read-only connections").
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("fabriqtest: start standby container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(standby); termErr != nil {
			t.Logf("fabriqtest: terminate standby: %v", termErr)
		}
	})

	primaryDSN = containerDSN(t, ctx, primary)
	standbyDSN = containerDSN(t, ctx, standby)

	// Prove replication is live before handing the pair back: write a sentinel
	// on the primary and poll until the standby (a hot-standby read replica)
	// serves it. The poll uses a fault-tolerant query — until the CREATE TABLE
	// has replicated, the standby answers "relation does not exist", which is a
	// transient condition to retry through rather than a fatal error.
	ApplyDDL(t, primaryDSN, []string{
		`CREATE TABLE IF NOT EXISTS _repl_ready (id int primary key)`,
		`INSERT INTO _repl_ready VALUES (1) ON CONFLICT DO NOTHING`,
	})
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, qErr := tryQueryStrings(standbyDSN, `SELECT id::text FROM _repl_ready WHERE id = 1`)
		if qErr == nil && len(rows) == 1 && rows[0] == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fabriqtest: replication never established primary->standby within timeout (last err: %v)", qErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return primaryDSN, standbyDSN
}

// tryQueryStrings runs a single-column query and returns rows or an error
// WITHOUT failing the test — the replication-readiness poll retries through the
// transient "relation does not exist" the standby returns before DDL has
// streamed across.
func tryQueryStrings(dsn, sql string) ([]string, error) {
	ctx := context.Background()
	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// containerDSN builds a sslmode=disable DSN from a running postgres container's
// mapped host and port.
func containerDSN(t *testing.T, ctx context.Context, c testcontainers.Container) string {
	t.Helper()
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("fabriqtest: container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("fabriqtest: container mapped port: %v", err)
	}
	return fmt.Sprintf("postgres://fabriq:fabriq@%s:%s/fabriq?sslmode=disable", host, port.Port())
}
