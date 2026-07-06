package postgres

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/migrations"
)

// ClusterOps implements provision.ClusterOps against real Postgres
// clusters: CREATE DATABASE over a maintenance connection, and fabriq's
// migration chain over a short-lived connection to the tenant database.
// Both operations are idempotent — the provisioning state machine's
// resumability rests on it.
type ClusterOps struct {
	// clusterDSNs maps cluster ids to server-level DSNs (a maintenance
	// database such as "postgres"; Config.Catalog.ClusterDSNs).
	clusterDSNs map[string]string
}

var _ provision.ClusterOps = (*ClusterOps)(nil)

// NewClusterOps builds ClusterOps over the configured clusters.
func NewClusterOps(clusterDSNs map[string]string) *ClusterOps {
	copied := make(map[string]string, len(clusterDSNs))
	for id, dsn := range clusterDSNs {
		copied[id] = dsn
	}
	return &ClusterOps{clusterDSNs: copied}
}

// dbIdent restricts database names to safe identifiers (they are
// interpolated into CREATE DATABASE, which cannot be parameterized).
var dbIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (c *ClusterOps) clusterDSN(clusterID string) (string, error) {
	dsn, ok := c.clusterDSNs[clusterID]
	if !ok {
		return "", fabriqerr.New(fabriqerr.CodeInvalidInput,
			"unknown cluster.", fabriqerr.WithEntity("cluster", clusterID))
	}
	return dsn, nil
}

// AssertBoot fails fast on cluster misconfiguration at Open time instead
// of per request (spec P6): every configured cluster must dial, and the
// serving credentials must not be superuser (RLS inside a tenant database
// does not bind superusers) unless explicitly allowed for dev/test.
// Clusters are checked in id order so the first error is deterministic.
func (c *ClusterOps) AssertBoot(ctx context.Context, allowSuperuser bool) error {
	ids := make([]string, 0, len(c.clusterDSNs))
	for id := range c.clusterDSNs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		db := pgdriver.New()
		if err := db.Open(ctx, c.clusterDSNs[id]); err != nil {
			return fmt.Errorf("fabriq: catalog boot check: cluster %q does not dial: %w", id, err)
		}
		var super string
		err := db.QueryRow(ctx, `SELECT current_setting('is_superuser')`).Scan(&super)
		_ = db.Close()
		if err != nil {
			// grove opens lazily; the first query IS the dial.
			return fmt.Errorf("fabriq: catalog boot check: cluster %q does not dial: %w", id, err)
		}
		if super == "on" && !allowSuperuser {
			return fmt.Errorf("fabriq: catalog boot check: cluster %q credentials are superuser — RLS does not bind superusers; serve with a dedicated role (catalog.allowSuperuser overrides for dev/test)", id)
		}
	}
	return nil
}

// PingDSN opens a short-lived connection to dsn and runs SELECT 1, reporting
// reachability. It is the reachability probe behind the adminapi connection-info
// endpoints (per-cluster and per-tenant DBs, which have no persistent adapter).
// Bound it with a context deadline; grove dials lazily, so the query is the dial.
func PingDSN(ctx context.Context, dsn string) error {
	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var one int
	return db.QueryRow(ctx, `SELECT 1`).Scan(&one)
}

// TenantDSN derives the DSN for one tenant database on a cluster — the
// same derivation the catalog-mode dialer uses, exported so routing and
// provisioning can never disagree.
func (c *ClusterOps) TenantDSN(clusterID, database string) (string, error) {
	dsn, err := c.clusterDSN(clusterID)
	if err != nil {
		return "", err
	}
	return dsnWithDatabase(dsn, database)
}

// dsnWithDatabase swaps the database path of a postgres:// DSN.
func dsnWithDatabase(dsn, database string) (string, error) {
	if !dbIdent.MatchString(database) {
		return "", fabriqerr.New(fabriqerr.CodeInvalidInput,
			"invalid database name.", fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"database": database}}))
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "", fabriqerr.New(fabriqerr.CodeInvalidInput,
			"cluster DSN must be a postgres:// URL.")
	}
	u.Path = "/" + database
	return u.String(), nil
}

// CreateDatabase implements provision.ClusterOps (idempotent: an existing
// database is success).
func (c *ClusterOps) CreateDatabase(ctx context.Context, clusterID, database string) error {
	if !dbIdent.MatchString(database) {
		return fabriqerr.New(fabriqerr.CodeInvalidInput, "invalid database name.")
	}
	dsn, err := c.clusterDSN(clusterID)
	if err != nil {
		return err
	}
	db := pgdriver.New()
	if openErr := db.Open(ctx, dsn); openErr != nil {
		return fmt.Errorf("fabriq: dial cluster %s: %w", clusterID, openErr)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(ctx, `SELECT 1 FROM pg_database WHERE datname = $1`, database)
	if err != nil {
		return translatePg("provision exists-check", "pg_database", database, err)
	}
	exists := rows.Next()
	_ = rows.Close()
	if exists {
		return nil
	}
	// CREATE DATABASE cannot be parameterized; database passed dbIdent.
	if _, err := db.Exec(ctx, `CREATE DATABASE `+database); err != nil {
		// A concurrent provisioner may have won the race.
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return translatePg("provision create database", "pg_database", database, err)
	}
	return nil
}

// Migrate implements provision.ClusterOps: run fabriq's chain against the
// tenant database and report the head version.
func (c *ClusterOps) Migrate(ctx context.Context, clusterID, database string) (string, error) {
	dsn, err := c.TenantDSN(clusterID, database)
	if err != nil {
		return "", err
	}
	db := pgdriver.New()
	if openErr := db.Open(ctx, dsn); openErr != nil {
		return "", fmt.Errorf("fabriq: dial tenant db %s/%s: %w", clusterID, database, openErr)
	}
	defer func() { _ = db.Close() }()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		return "", err
	}
	if _, err := orch.Migrate(ctx); err != nil {
		return "", fmt.Errorf("fabriq: migrate tenant db %s/%s: %w", clusterID, database, err)
	}
	return migrations.HeadVersion(), nil
}

var _ provision.SchemaClusterOps = (*ClusterOps)(nil)

// EnsureBootstrap implements provision.SchemaClusterOps: prepare a
// consolidation database ONCE with the shared schema and its extensions, so
// every tenant schema resolves pgvector/postgis types via search_path.
// Idempotent — a re-run is a cheap marker check plus IF NOT EXISTS DDL.
func (c *ClusterOps) EnsureBootstrap(ctx context.Context, clusterID, database, sharedSchema string) error {
	if !dbIdent.MatchString(sharedSchema) {
		return fabriqerr.New(fabriqerr.CodeInvalidInput, "invalid shared schema name.",
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"schema": sharedSchema}}))
	}
	dsn, err := c.TenantDSN(clusterID, database)
	if err != nil {
		return err
	}
	db := pgdriver.New()
	if openErr := db.Open(ctx, dsn); openErr != nil {
		return fmt.Errorf("fabriq: dial consolidation db %s/%s: %w", clusterID, database, openErr)
	}
	defer func() { _ = db.Close() }()

	// sharedSchema passed dbIdent, so it is safe to interpolate into DDL that
	// cannot be parameterized.
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + sharedSchema,
		`CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA ` + sharedSchema,
		`CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA ` + sharedSchema,
		`CREATE TABLE IF NOT EXISTS ` + sharedSchema + `.fabriq_bootstrap (
			id int PRIMARY KEY,
			bootstrapped_at timestamptz NOT NULL DEFAULT now()
		)`,
		`INSERT INTO ` + sharedSchema + `.fabriq_bootstrap (id) VALUES (1) ON CONFLICT DO NOTHING`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			return translatePg("provision bootstrap", sharedSchema, "", err)
		}
	}
	return nil
}

// CreateSchema implements provision.SchemaClusterOps (idempotent).
func (c *ClusterOps) CreateSchema(ctx context.Context, clusterID, database, schema string) error {
	if !dbIdent.MatchString(schema) {
		return fabriqerr.New(fabriqerr.CodeInvalidInput, "invalid schema name.",
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"schema": schema}}))
	}
	dsn, err := c.TenantDSN(clusterID, database)
	if err != nil {
		return err
	}
	db := pgdriver.New()
	if openErr := db.Open(ctx, dsn); openErr != nil {
		return fmt.Errorf("fabriq: dial consolidation db %s/%s: %w", clusterID, database, openErr)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		return translatePg("provision create schema", schema, "", err)
	}
	return nil
}

// MigrateSchema implements provision.SchemaClusterOps: run fabriq's chain
// inside a tenant schema. search_path is baked into the connection string so
// EVERY pooled connection resolves bare names (and grove_migrations) to the
// tenant schema, with the shared schema appended so the chain's
// CREATE EXTENSION IF NOT EXISTS steps no-op against the already-installed
// extensions.
func (c *ClusterOps) MigrateSchema(ctx context.Context, clusterID, database, schema, sharedSchema string) (string, error) {
	if !dbIdent.MatchString(schema) || !dbIdent.MatchString(sharedSchema) {
		return "", fabriqerr.New(fabriqerr.CodeInvalidInput, "invalid schema name.")
	}
	dsn, err := c.TenantDSN(clusterID, database)
	if err != nil {
		return "", err
	}
	dsn, err = dsnWithSearchPath(dsn, schema, sharedSchema)
	if err != nil {
		return "", err
	}
	db := pgdriver.New()
	if openErr := db.Open(ctx, dsn); openErr != nil {
		return "", fmt.Errorf("fabriq: dial consolidation db %s/%s: %w", clusterID, database, openErr)
	}
	defer func() { _ = db.Close() }()

	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		return "", err
	}
	if _, err := orch.Migrate(ctx); err != nil {
		return "", fmt.Errorf("fabriq: migrate schema %s/%s/%s: %w", clusterID, database, schema, err)
	}
	return migrations.HeadVersion(), nil
}

// dsnWithSearchPath appends a connection-level search_path (via the libpq
// "options" runtime parameter) so every connection in the pool defaults to
// "<schema>, <shared>". This is how migrations — which span many statements
// and may use multiple connections — land in the tenant schema.
func dsnWithSearchPath(dsn, schema, shared string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return "", fabriqerr.New(fabriqerr.CodeInvalidInput, "cluster DSN must be a postgres:// URL.")
	}
	q := u.Query()
	q.Set("options", "-c search_path="+schema+","+shared)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
