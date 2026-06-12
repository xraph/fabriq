package main

import (
	"fmt"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/migrations"
)

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

	_ = cmd.AddSubcommand(up)
	_ = cmd.AddSubcommand(down)
	_ = cmd.AddSubcommand(status)
	return cmd
}
