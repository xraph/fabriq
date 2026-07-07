package main

import (
	"fmt"
	"os"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/pganalytics"
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

	purge := cli.NewCommand("purge", "Erase one tenant's data from the analytics sink (offboarding / right-to-be-forgotten)", func(ctx cli.CommandContext) error {
		tenantID := ctx.String("tenant")
		if tenantID == "" {
			return cliError("--tenant is required")
		}
		if !ctx.Bool("yes") {
			return cliError(fmt.Sprintf("refusing to erase tenant %q without --yes (this permanently deletes its facts, events, and watermarks)", tenantID))
		}
		analyticsDSN := ctx.String("analytics-dsn")
		if analyticsDSN == "" {
			analyticsDSN = os.Getenv("FABRIQ_ANALYTICS_DSN")
		}
		if analyticsDSN == "" {
			return cliError("--analytics-dsn (or FABRIQ_ANALYTICS_DSN) is required")
		}
		as, err := pganalytics.Open(ctx.Context(), analyticsDSN)
		if err != nil {
			return err
		}
		defer func() { _ = as.Close() }()
		n, err := as.PurgeTenant(ctx.Context(), tenantID)
		if err != nil {
			return err
		}
		ctx.Success(fmt.Sprintf("tenant %s: erased %d analytics rows", tenantID, n))
		return nil
	})
	purge.AddFlag(cli.NewStringFlag("analytics-dsn", "", "analytics sink DSN (or FABRIQ_ANALYTICS_DSN)", ""))
	purge.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id to erase", ""))
	purge.AddFlag(cli.NewBoolFlag("yes", "", "confirm the destructive erase", false))

	reproject := cli.NewCommand("reproject", "Re-apply the current redaction allow-list to already-stored rows", func(ctx cli.CommandContext) error {
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

		rp, err := stores.AnalyticsReprojector(r)
		if err != nil {
			return err
		}

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

		if !all {
			n, err := rp.Tenant(ctx.Context(), tenants[0])
			if err != nil {
				return err
			}
			ctx.Success(fmt.Sprintf("tenant %s: reprojected %d rows", tenants[0], n))
			return nil
		}
		counts, err := rp.AllTenants(ctx.Context(), tenants, ctx.Int("concurrency"))
		for _, tenantID := range tenants {
			ctx.Println(fmt.Sprintf("%-24s reprojected %d rows", tenantID, counts[tenantID]))
		}
		if err != nil {
			return err
		}
		ctx.Success(fmt.Sprintf("reprojected %d tenants", len(tenants)))
		return nil
	})
	reproject.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	reproject.AddFlag(cli.NewStringFlag("analytics-dsn", "", "analytics sink DSN (or FABRIQ_ANALYTICS_DSN)", ""))
	reproject.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	reproject.AddFlag(cli.NewBoolFlag("all-tenants", "", "reproject every tenant", false))
	reproject.AddFlag(cli.NewIntFlag("concurrency", "", "concurrent tenant reprojections (--all-tenants only)", 4))

	_ = cmd.AddSubcommand(backfill)
	_ = cmd.AddSubcommand(purge)
	_ = cmd.AddSubcommand(reproject)
	return cmd
}
