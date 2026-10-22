//go:build integration

package migrations_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// dsnAs rewrites a DSN to connect as the given role.
func dsnAs(t *testing.T, dsn, role, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.User = url.UserPassword(role, password)
	return u.String()
}

// connectAs opens a connection as the role and reports the dial or ping error.
func connectAs(dsn string) (*pgdriver.PgDB, error) {
	db := pgdriver.New()
	ctx := context.Background()
	if err := db.Open(ctx, dsn); err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func migrated(t *testing.T) (superDSN string, super *pgdriver.PgDB) {
	t.Helper()
	superDSN = fabriqtest.StartPostgres(t)
	super = pgdriver.New()
	if err := super.Open(context.Background(), superDSN); err != nil {
		t.Fatalf("open super: %v", err)
	}
	t.Cleanup(func() { _ = super.Close() })
	orch, err := migrations.NewOrchestrator(super)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return superDSN, super
}

// roleFlags reads the attributes that decide whether a role escapes RLS or can
// mint more privileged roles.
func roleFlags(t *testing.T, db *pgdriver.PgDB, role string) (super, bypass, createdb, createrole, repl bool) {
	t.Helper()
	row := db.QueryRow(context.Background(),
		`SELECT rolsuper, rolbypassrls, rolcreatedb, rolcreaterole, rolreplication FROM pg_roles WHERE rolname = $1::text`, role)
	if err := row.Scan(&super, &bypass, &createdb, &createrole, &repl); err != nil {
		t.Fatalf("read pg_roles for %s: %v", role, err)
	}
	return
}

func assertSafeRole(t *testing.T, db *pgdriver.PgDB, role string) {
	t.Helper()
	su, by, cdb, crr, rep := roleFlags(t, db, role)
	if su || by || cdb || crr || rep {
		t.Fatalf("role %s: super=%v bypassrls=%v createdb=%v createrole=%v replication=%v, want all false", role, su, by, cdb, crr, rep)
	}
}

func TestEnsureAppRole_CreatesNonBypassRole(t *testing.T) {
	ctx := context.Background()
	superDSN, super := migrated(t)

	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "pw-one"); err != nil {
		t.Fatalf("EnsureAppRole: %v", err)
	}

	assertSafeRole(t, super, "fabriq_app")

	app, err := connectAs(dsnAs(t, superDSN, "fabriq_app", "pw-one"))
	if err != nil {
		t.Fatalf("connect as fabriq_app: %v", err)
	}
	defer func() { _ = app.Close() }()

	if _, err := app.Exec(ctx, `CREATE TABLE t_probe (id int)`); err != nil {
		t.Fatalf("fabriq_app cannot create a table: %v", err)
	}
	var n int
	if err := app.QueryRow(ctx, `SELECT count(*) FROM fabriq_outbox`).Scan(&n); err != nil {
		t.Fatalf("fabriq_app cannot read fabriq_outbox: %v", err)
	}
}

func TestEnsureAppRole_GrantsFutureTables(t *testing.T) {
	ctx := context.Background()
	superDSN, super := migrated(t)

	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "pw-one"); err != nil {
		t.Fatalf("EnsureAppRole: %v", err)
	}
	// A table the privileged role creates after the grant is covered by the
	// default privileges.
	if _, err := super.Exec(ctx, `CREATE TABLE t_later (id serial PRIMARY KEY, v int)`); err != nil {
		t.Fatalf("create t_later: %v", err)
	}
	app, err := connectAs(dsnAs(t, superDSN, "fabriq_app", "pw-one"))
	if err != nil {
		t.Fatalf("connect as fabriq_app: %v", err)
	}
	defer func() { _ = app.Close() }()
	if _, err := app.Exec(ctx, `INSERT INTO t_later (v) VALUES (1)`); err != nil {
		t.Fatalf("fabriq_app cannot write a later table: %v", err)
	}
}

func TestEnsureAppRole_Idempotent_RotatesPassword(t *testing.T) {
	ctx := context.Background()
	superDSN, super := migrated(t)

	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "old-pw"); err != nil {
		t.Fatalf("first EnsureAppRole: %v", err)
	}
	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "new-pw"); err != nil {
		t.Fatalf("second EnsureAppRole: %v", err)
	}

	assertSafeRole(t, super, "fabriq_app")
	if db, err := connectAs(dsnAs(t, superDSN, "fabriq_app", "old-pw")); err == nil {
		_ = db.Close()
		t.Fatal("old password still connects after rotation")
	}
	db, err := connectAs(dsnAs(t, superDSN, "fabriq_app", "new-pw"))
	if err != nil {
		t.Fatalf("new password does not connect: %v", err)
	}
	_ = db.Close()
}

func TestEnsureAppRole_QuotesHostilePassword(t *testing.T) {
	ctx := context.Background()
	superDSN, _ := migrated(t)

	pw := `p'w"; DROP TABLE fabriq_outbox; --\`
	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", pw); err != nil {
		t.Fatalf("EnsureAppRole: %v", err)
	}
	db, err := connectAs(dsnAs(t, superDSN, "fabriq_app", pw))
	if err != nil {
		t.Fatalf("connect with hostile password: %v", err)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM fabriq_outbox`).Scan(&n); err != nil {
		t.Fatalf("outbox damaged or unreadable: %v", err)
	}
	_ = db.Close()
}

func TestEnsureAppRole_RejectsBadRoleName(t *testing.T) {
	// The DSN is unreachable: validation must fail before any connection.
	const dsn = "postgres://nobody:pw@127.0.0.1:1/none?sslmode=disable&connect_timeout=1"
	for _, role := range []string{"Bad-Role", "x; drop", "", "1abc", strings.Repeat("a", 64)} {
		err := migrations.EnsureAppRole(context.Background(), dsn, role, "pw")
		if err == nil {
			t.Fatalf("role %q accepted", role)
		}
		if !strings.Contains(err.Error(), "invalid") {
			t.Fatalf("role %q: error %q does not say the name is invalid (did it try to connect?)", role, err)
		}
	}
	if err := migrations.EnsureAppRole(context.Background(), dsn, "fabriq_app", ""); err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("empty password: err = %v, want a password error", err)
	}
}

func TestEnsureAppRole_ErrorDoesNotLeakPassword(t *testing.T) {
	ctx := context.Background()
	superDSN, _ := migrated(t)
	// Connect as a role that cannot create roles: the statement fails.
	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "pw-a"); err != nil {
		t.Fatal(err)
	}
	err := migrations.EnsureAppRole(ctx, dsnAs(t, superDSN, "fabriq_app", "pw-a"), "other_role", "leak-me-not-123")
	if err == nil {
		t.Fatal("expected a permission error from an unprivileged connection")
	}
	if strings.Contains(err.Error(), "leak-me-not-123") {
		t.Fatalf("error leaks the password: %v", err)
	}
}

func TestEnsureAppRole_ResetsPreexistingDangerousRole(t *testing.T) {
	ctx := context.Background()
	superDSN, super := migrated(t)

	if _, err := super.Exec(ctx, `CREATE ROLE fabriq_app LOGIN PASSWORD 'old-pw' SUPERUSER BYPASSRLS CREATEDB CREATEROLE REPLICATION`); err != nil {
		t.Fatalf("create dangerous role: %v", err)
	}
	if su, by, cdb, crr, rep := roleFlags(t, super, "fabriq_app"); !su || !by || !cdb || !crr || !rep {
		t.Fatalf("precondition: flags super=%v bypassrls=%v createdb=%v createrole=%v replication=%v, want all true", su, by, cdb, crr, rep)
	}

	if err := migrations.EnsureAppRole(ctx, superDSN, "fabriq_app", "new-pw"); err != nil {
		t.Fatalf("EnsureAppRole: %v", err)
	}
	assertSafeRole(t, super, "fabriq_app")
}

func TestEnsureAppRole_RefusesConnectingRole(t *testing.T) {
	ctx := context.Background()
	superDSN, _ := migrated(t)

	// StartPostgres connects as "fabriq" with password "fabriq".
	err := migrations.EnsureAppRole(ctx, superDSN, "fabriq", "locked-out-pw")
	if !errors.Is(err, migrations.ErrInvalidAppRole) {
		t.Fatalf("err = %v, want ErrInvalidAppRole", err)
	}
	// The privileged role's password must be untouched.
	db, cerr := connectAs(superDSN)
	if cerr != nil {
		t.Fatalf("privileged role locked out: %v", cerr)
	}
	_ = db.Close()
}
