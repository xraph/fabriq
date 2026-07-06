package main

import (
	"fmt"
	"os"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

// analyticsCommand is the cross-tenant analytics sink operator surface:
//
//	fabriq analytics backfill --tenant <id> [--analytics-dsn <dsn>]
//	fabriq analytics backfill --all-tenants [--concurrency N]
//
// backfill replays each tenant's current-state snapshot through the same
// applier the live proj:analytics consumer uses, so a re-run is a no-op
// (version-gated) and can run concurrently with live traffic.
func analyticsCommand() cli.Command {
	cmd := cli.NewCommand("analytics", "Cross-tenant analytics sink operations", func(ctx cli.CommandContext) error {
		ctx.Println("usage: fabriq analytics backfill")
		return nil
	})

	backfill := cli.NewCommand("backfill", "Replay tenant snapshots into the analytics sink", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		analyticsDSN := ctx.String("analytics-dsn")
		if analyticsDSN == "" {
			analyticsDSN = os.Getenv("FABRIQ_ANALYTICS_DSN")
		}
		if analyticsDSN == "" {
			return cliError("--analytics-dsn (or FABRIQ_ANALYTICS_DSN) is required")
		}

		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
			return err
		}

		cfg := fabriq.Config{
			Postgres:  fabriq.PostgresConfig{DSN: dsn},
			Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
		}
		_, stores, err := fabriq.Open(ctx.Context(), r, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = stores.Close() }()

		tenants := []string{ctx.String("tenant")}
		all := ctx.Bool("all-tenants")
		if all {
			tenants, err = stores.AllTenants(ctx.Context())
			if err != nil {
				return err
			}
		} else if tenants[0] == "" {
			return cliError("--tenant is required (or pass --all-tenants)")
		}

		bf, err := stores.AnalyticsBackfiller(r)
		if err != nil {
			return err
		}

		if !all {
			n, err := bf.Tenant(ctx.Context(), tenants[0])
			if err != nil {
				return err
			}
			ctx.Success(fmt.Sprintf("tenant %s: backfilled %d rows", tenants[0], n))
			return nil
		}

		concurrency := ctx.Int("concurrency")
		counts, err := bf.AllTenants(ctx.Context(), tenants, concurrency)
		for _, tenantID := range tenants {
			ctx.Println(fmt.Sprintf("%-24s backfilled %d rows", tenantID, counts[tenantID]))
		}
		if err != nil {
			return err
		}
		ctx.Success(fmt.Sprintf("backfilled %d tenants", len(tenants)))
		return nil
	})
	backfill.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	backfill.AddFlag(cli.NewStringFlag("analytics-dsn", "", "analytics sink DSN (or FABRIQ_ANALYTICS_DSN)", ""))
	backfill.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	backfill.AddFlag(cli.NewBoolFlag("all-tenants", "", "backfill every tenant", false))
	backfill.AddFlag(cli.NewIntFlag("concurrency", "", "concurrent tenant backfills (--all-tenants only)", 4))

	_ = cmd.AddSubcommand(backfill)
	return cmd
}
