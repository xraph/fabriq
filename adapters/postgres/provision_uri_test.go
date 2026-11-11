package postgres

import (
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// DSNWithSearchPathForTest lets the external adapter tests use the same URI as migrations.
var DSNWithSearchPathForTest = dsnWithSearchPath

func TestDSNWithSearchPath(t *testing.T) {
	const clusterDSN = "postgresql://fixture+user:fixture%2Bpass%40word@127.0.0.1:5432/maintenance?" +
		"sslmode=disable&application_name=first&options=-c%20statement_timeout%3D1" +
		"&fabriq.literal_plus=left+right&fabriq.encoded_plus=left%2Bright" +
		"&fabriq.space=left%20right&fabriq.delimiters=left%26middle%3Dright%23end" +
		"&application_name=fabriq%20test&opt%69ons=-c%20statement_timeout%3D2"

	ops := NewClusterOps(map[string]string{"test": clusterDSN})
	tenantDSN, err := ops.TenantDSN("test", "tenant_database")
	if err != nil {
		t.Fatal("could not build the tenant database URI")
	}
	dsn, err := dsnWithSearchPath(tenantDSN, "tenant_schema", "shared_schema")
	if err != nil {
		t.Fatal("could not build the schema URI")
	}
	pool, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("pgx could not parse the schema URI")
	}

	conn := pool.ConnConfig
	if conn.Database != "tenant_database" || conn.Host != "127.0.0.1" || conn.Port != 5432 {
		t.Error("schema URI changed the tenant database address")
	}
	if conn.User != "fixture+user" || conn.Password != "fixture+pass@word" {
		t.Error("schema URI changed the credentials")
	}
	for key, want := range map[string]string{
		"options":             "-c search_path=tenant_schema,shared_schema",
		"application_name":    "fabriq test",
		"fabriq.literal_plus": "left+right",
		"fabriq.encoded_plus": "left+right",
		"fabriq.space":        "left right",
		"fabriq.delimiters":   "left&middle=right#end",
	} {
		if got := conn.RuntimeParams[key]; got != want {
			t.Errorf("pgx runtime parameter %s = %q, want %q", key, got, want)
		}
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("could not parse the resulting schema URI")
	}
	if got := len(parsed.Query()["options"]); got != 1 {
		t.Errorf("schema URI has %d options parameters, want 1", got)
	}
}

func TestDSNWithSearchPath_InvalidURI(t *testing.T) {
	const dsn = "postgresql://fixture:fixture%ZZpassword@127.0.0.1:5432/tenant_database"
	got, err := dsnWithSearchPath(dsn, "tenant_schema", "shared_schema")
	if got != "" || fabriqerr.CodeOf(err) != fabriqerr.CodeInvalidInput {
		t.Fatal("invalid cluster URI should return an empty URI and invalid_input")
	}
	if err.Error() != "fabriq: invalid_input: cluster DSN must be a postgres:// URL." {
		t.Fatal("invalid cluster URI should return the generic error")
	}
}
