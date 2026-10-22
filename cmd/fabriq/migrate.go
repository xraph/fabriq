package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/migrations"
)

// Seams for the unit tests: the commands reach Postgres only through these.
var (
	checkExtensions = migrations.CheckExtensions
	ensureAppRole   = migrations.EnsureAppRole
)

// Exit codes the deployment tooling branches on.
const (
	exitUsage         = 2 // missing or invalid flag or environment value
	exitPreflightFail = 4 // a required extension is missing or not preloaded
)

// appRolePasswordEnv carries the app role password. It is read from the
// environment only, so it never lands in argv or a process listing.
//
// #nosec G101 -- an environment variable name, not a credential.
const appRolePasswordEnv = "FABRIQ_APP_ROLE_PASSWORD"

func migrateCommand() cli.Command {
	cmd := cli.NewCommand("migrate", "Run fabriq's grove migrations", func(ctx cli.CommandContext) error {
		ctx.Println("usage: fabriq migrate up|down|status")
		return nil
	})
	cmd.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))

	up := cli.NewCommand("up", "Apply all pending migrations", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		orch, closeFn, err := migrations.OpenOrchestrator(ctx.Context(), dsn)
		if err != nil {
			return err
		}
		defer func() { _ = closeFn() }()

		res, err := orch.Migrate(ctx.Context())
		if err != nil {
			return err
		}
		if len(res.Applied) == 0 {
			ctx.Info("database is up to date")
			return nil
		}
		for _, m := range res.Applied {
			ctx.Success(fmt.Sprintf("applied %s %s (%s)", m.Version, m.Name, m.Group))
		}
		return nil
	})
	up.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))

	down := cli.NewCommand("down", "Roll back the most recent migration", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		orch, closeFn, err := migrations.OpenOrchestrator(ctx.Context(), dsn)
		if err != nil {
			return err
		}
		defer func() { _ = closeFn() }()

		res, err := orch.Rollback(ctx.Context())
		if err != nil {
			return err
		}
		if len(res.Rollback) == 0 {
			ctx.Info("nothing to roll back")
			return nil
		}
		for _, m := range res.Rollback {
			ctx.Warning(fmt.Sprintf("rolled back %s %s (%s)", m.Version, m.Name, m.Group))
		}
		return nil
	})
	down.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))

	status := cli.NewCommand("status", "Show applied and pending migrations", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		orch, closeFn, err := migrations.OpenOrchestrator(ctx.Context(), dsn)
		if err != nil {
			return err
		}
		defer func() { _ = closeFn() }()

		groups, err := orch.Status(ctx.Context())
		if err != nil {
			return err
		}
		for _, g := range groups {
			ctx.Println(fmt.Sprintf("group %s: %d applied, %d pending", g.Name, len(g.Applied), len(g.Pending)))
			for _, m := range g.Applied {
				ctx.Println(fmt.Sprintf("  [x] %s %s (%s)", m.Migration.Version, m.Migration.Name, m.AppliedAt))
			}
			for _, m := range g.Pending {
				ctx.Println(fmt.Sprintf("  [ ] %s %s", m.Migration.Version, m.Migration.Name))
			}
		}
		return nil
	})
	status.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))

	preflight := cli.NewCommand("preflight", "Check the database has the extensions fabriq needs (exit 4 when it does not)", runPreflight)
	preflight.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN), a privileged role", ""))
	preflight.AddFlag(cli.NewStringFlag("require", "", "Comma-separated extensions to require", strings.Join(migrations.DefaultRequiredExtensions, ",")))
	preflight.AddFlag(cli.NewBoolFlag("json", "", "Print the report as JSON", false))

	appRole := cli.NewCommand("app-role", "Create or update the non-superuser role fabriq serve connects as (password from "+appRolePasswordEnv+")", runAppRole)
	appRole.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN), a privileged role", ""))
	appRole.AddFlag(cli.NewStringFlag("role", "", "Role name, for example fabriq_app", ""))

	_ = cmd.AddSubcommand(up)
	_ = cmd.AddSubcommand(down)
	_ = cmd.AddSubcommand(status)
	_ = cmd.AddSubcommand(preflight)
	_ = cmd.AddSubcommand(appRole)
	return cmd
}

// splitList splits a comma-separated flag value, trimming blanks and dropping
// empty items.
func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// runPreflight prints the extension report. Exit 0 when every required
// extension is usable, 4 when any is not, 1 when the check itself could not run.
func runPreflight(ctx cli.CommandContext) error {
	dsn, ok := dsnFromContext(ctx)
	if !ok {
		return errMissingDSN
	}
	required := splitList(ctx.String("require"))
	if len(required) == 0 {
		return cli.NewError("--require lists no extensions", exitUsage)
	}

	rep, err := checkExtensions(ctx.Context(), dsn, required)
	if err != nil {
		return err
	}

	if ctx.Bool("json") {
		b, err := json.Marshal(rep)
		if err != nil {
			return err
		}
		ctx.Println(string(b))
	} else {
		for _, e := range rep.Extensions {
			state := "ok"
			if !e.OK {
				state = "FAIL"
			}
			installed := e.Installed
			if installed == "" {
				installed = "-"
			}
			ctx.Println(fmt.Sprintf("%-4s %s available=%t installed=%s preloaded=%t", state, e.Name, e.Available, installed, e.Preloaded))
		}
		for _, p := range rep.Problems {
			ctx.Println("problem: " + p)
		}
	}
	if !rep.OK {
		return cli.NewError("preflight failed: "+strings.Join(rep.Problems, "; "), exitPreflightFail)
	}
	return nil
}

// runAppRole creates or rotates the app role. Exit 0 on success, 2 for a
// missing or invalid role or password, 1 for a database error.
func runAppRole(ctx cli.CommandContext) error {
	role := ctx.String("role")
	if role == "" {
		return cli.NewError("--role is required", exitUsage)
	}
	password := os.Getenv(appRolePasswordEnv)
	if password == "" {
		return cli.NewError(appRolePasswordEnv+" is required", exitUsage)
	}
	if err := migrations.ValidateAppRole(role, password); err != nil {
		return cli.WrapError(err, "invalid app role", exitUsage)
	}
	dsn, ok := dsnFromContext(ctx)
	if !ok {
		return errMissingDSN
	}

	if err := ensureAppRole(ctx.Context(), dsn, role, password); err != nil {
		if errors.Is(err, migrations.ErrInvalidAppRole) {
			return cli.WrapError(err, "invalid app role", exitUsage)
		}
		return err
	}
	ctx.Success(fmt.Sprintf("app role %s is ready", role))
	return nil
}
