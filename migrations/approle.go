package migrations

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/xraph/grove/drivers/pgdriver"
)

// ErrInvalidAppRole marks a role name or password that EnsureAppRole refuses
// before it touches the database. Callers map it to a usage error.
var ErrInvalidAppRole = errors.New("fabriq: invalid app role")

var appRoleName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidateAppRole checks the role name and password EnsureAppRole will use,
// without touching the database. The error never contains the password.
func ValidateAppRole(role, password string) error {
	if !appRoleName.MatchString(role) {
		return fmt.Errorf("%w: role name %q is invalid, want lowercase letters, digits and underscores, starting with a letter or underscore, at most 63 characters", ErrInvalidAppRole, role)
	}
	if password == "" {
		return fmt.Errorf("%w: password is empty", ErrInvalidAppRole)
	}
	return nil
}

// EnsureAppRole creates the non-superuser application role, or rotates its
// password when it already exists, and grants it what `fabriq serve` needs on
// the public schema. Run it with a privileged DSN after migrations.
//
// Row-level security does not bind superusers or BYPASSRLS roles, and a
// REPLICATION role can read around it too, so serving as the migration owner
// would turn the tenant fence off. For that reason an existing role is not just
// re-passworded: SUPERUSER, BYPASSRLS, CREATEDB, CREATEROLE and REPLICATION are
// all reset, so a role that was once over-privileged comes back safe. The role
// must not be the one the DSN connects as, or the command could lock it out. The role gets CREATE
// on the schema so serve can create dynamic tables as their owner, and FORCE
// ROW LEVEL SECURITY then binds that owner.
//
// The role name and password never pass through Go string formatting into SQL:
// each statement is assembled by the server with format(%I, %L) and then
// executed. Running it again is safe and is how a password is rotated.
func EnsureAppRole(ctx context.Context, dsn, role, password string) error {
	if err := ValidateAppRole(role, password); err != nil {
		return err
	}

	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn); err != nil {
		return fmt.Errorf("fabriq: app role: connect to postgres: %w", redactSecrets(err, dsn, password))
	}
	defer func() { _ = pg.Close() }()

	var current string
	if err := pg.QueryRow(ctx, `SELECT current_user::text`).Scan(&current); err != nil {
		return fmt.Errorf("fabriq: app role: read connecting role: %w", redactSecrets(err, dsn, password))
	}
	if current == role {
		return fmt.Errorf("%w: role %q is the role this connection uses; run the command with a separate privileged DSN", ErrInvalidAppRole, role)
	}

	var exists bool
	if err := pg.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1::text)`, role).Scan(&exists); err != nil {
		return fmt.Errorf("fabriq: app role: look up role: %w", redactSecrets(err, dsn, password))
	}

	tmpl := `CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION`
	if exists {
		tmpl = `ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION`
	}
	var stmt string
	if err := pg.QueryRow(ctx, `SELECT format($1::text, $2::text, $3::text)`, tmpl, role, password).Scan(&stmt); err != nil {
		return fmt.Errorf("fabriq: app role: build role statement: %w", redactSecrets(err, dsn, password))
	}
	if _, err := pg.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("fabriq: app role: create or alter role: %w", redactSecrets(err, dsn, password))
	}

	grants := []string{
		`GRANT USAGE, CREATE ON SCHEMA public TO %I`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %I`,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %I`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %I`,
	}
	for _, g := range grants {
		var s string
		if err := pg.QueryRow(ctx, `SELECT format($1::text, $2::text)`, g, role).Scan(&s); err != nil {
			return fmt.Errorf("fabriq: app role: build grant: %w", redactSecrets(err, dsn, password))
		}
		if _, err := pg.Exec(ctx, s); err != nil {
			return fmt.Errorf("fabriq: app role: grant to %s: %w", role, redactSecrets(err, dsn, password))
		}
	}
	return nil
}

// redactSecrets scrubs the DSN and the password from an error message before
// it is wrapped, as a backstop in case a driver echoes a statement or a
// connection string. When neither appears the original error is returned, so
// errors.Is and errors.As still see its cause. Only a message that actually
// held a secret is replaced, and that replacement drops the chain on purpose.
func redactSecrets(err error, dsn, password string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	out := msg
	if dsn != "" {
		out = strings.ReplaceAll(out, dsn, "<dsn>")
	}
	if password != "" {
		out = strings.ReplaceAll(out, password, "<redacted>")
	}
	if out == msg {
		return err
	}
	return errors.New(out)
}
