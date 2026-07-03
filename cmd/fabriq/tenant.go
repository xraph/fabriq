package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/migrations"
)

// tenantCommand is the db-per-tenant operator surface (catalog mode):
//
//	fabriq tenant provision <id> --cluster <cluster>
//	fabriq tenant suspend   <id>
//	fabriq tenant resume    <id>
//	fabriq tenant list
//	fabriq tenant migrate-all [--batch N] [--max-failures N]
//
// The control-plane connection comes from --catalog-dsn or
// FABRIQ_CATALOG_DSN; clusters from --clusters "id=dsn,id=dsn" or
// FABRIQ_CLUSTER_DSNS. Provisioning is explicit and idempotent; fabriq
// never drops a database (suspend routes off, the drop is a human step).
func tenantCommand() cli.Command {
	cmd := cli.NewCommand("tenant", "db-per-tenant catalog operations", func(ctx cli.CommandContext) error {
		ctx.Println("usage: fabriq tenant provision|suspend|resume|list|migrate-all")
		return nil
	})
	cmd.AddFlag(cli.NewStringFlag("catalog-dsn", "", "control database DSN (or FABRIQ_CATALOG_DSN)", ""))
	cmd.AddFlag(cli.NewStringFlag("clusters", "", `cluster DSNs "id=dsn,id=dsn" (or FABRIQ_CLUSTER_DSNS)`, ""))

	provisionCmd := cli.NewCommand("provision", "Create (or resume) a tenant's dedicated database", func(ctx cli.CommandContext) error {
		tenantID, err := tenantArg(ctx)
		if err != nil {
			return err
		}
		clusterID := ctx.String("cluster")
		if clusterID == "" {
			return fmt.Errorf("--cluster is required")
		}
		p, closeFn, err := provisionerFromContext(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		entry, err := p.Provision(ctx.Context(), tenantID, clusterID)
		if err != nil {
			return err
		}
		ctx.Println(fmt.Sprintf("tenant %s: %s on %s/%s (version %s)",
			entry.TenantID, entry.State, entry.ClusterID, entry.Database, entry.Version))
		return nil
	})
	provisionCmd.AddFlag(cli.NewStringFlag("cluster", "", "target cluster id", ""))

	suspendCmd := cli.NewCommand("suspend", "Route a tenant off (database untouched)", func(ctx cli.CommandContext) error {
		tenantID, err := tenantArg(ctx)
		if err != nil {
			return err
		}
		p, closeFn, err := provisionerFromContext(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		entry, err := p.Suspend(ctx.Context(), tenantID)
		if err != nil {
			return err
		}
		ctx.Println(fmt.Sprintf("tenant %s: %s", entry.TenantID, entry.State))
		return nil
	})

	resumeCmd := cli.NewCommand("resume", "Re-activate a suspended tenant", func(ctx cli.CommandContext) error {
		tenantID, err := tenantArg(ctx)
		if err != nil {
			return err
		}
		p, closeFn, err := provisionerFromContext(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		entry, err := p.Resume(ctx.Context(), tenantID)
		if err != nil {
			return err
		}
		ctx.Println(fmt.Sprintf("tenant %s: %s", entry.TenantID, entry.State))
		return nil
	})

	listCmd := cli.NewCommand("list", "List catalog entries", func(ctx cli.CommandContext) error {
		cat, closeFn, err := catalogFromContext(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		cursor := catalog.Cursor("")
		for {
			page, next, err := cat.List(ctx.Context(), cursor, 200)
			if err != nil {
				return err
			}
			for _, e := range page {
				ctx.Println(fmt.Sprintf("%-24s %-10s %s/%s version=%s", e.TenantID, e.State, e.ClusterID, e.Database, e.Version))
			}
			if next == "" {
				return nil
			}
			cursor = next
		}
	})

	migrateAllCmd := cli.NewCommand("migrate-all", "Roll fabriq's migration chain across the fleet", func(ctx cli.CommandContext) error {
		p, closeFn, err := provisionerFromContext(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		report, err := p.MigrateAll(ctx.Context(), provision.MigrateAllOpts{
			Batch:         ctx.Int("batch"),
			MaxFailures:   ctx.Int("max-failures"),
			TargetVersion: migrations.HeadVersion(),
		})
		for _, r := range report.Results {
			switch {
			case r.Err != "":
				ctx.Println(fmt.Sprintf("%-24s FAILED: %s", r.TenantID, r.Err))
			case r.Skipped:
				ctx.Println(fmt.Sprintf("%-24s skipped (already %s)", r.TenantID, r.Version))
			default:
				ctx.Println(fmt.Sprintf("%-24s migrated to %s", r.TenantID, r.Version))
			}
		}
		ctx.Println(fmt.Sprintf("migrated=%d skipped=%d failed=%d", report.Migrated, report.Skipped, report.Failed))
		return err
	})
	migrateAllCmd.AddFlag(cli.NewIntFlag("batch", "", "concurrent tenant migrations", 8))
	migrateAllCmd.AddFlag(cli.NewIntFlag("max-failures", "", "stop after this many failed tenants", 3))

	_ = cmd.AddSubcommand(provisionCmd)
	_ = cmd.AddSubcommand(suspendCmd)
	_ = cmd.AddSubcommand(resumeCmd)
	_ = cmd.AddSubcommand(listCmd)
	_ = cmd.AddSubcommand(migrateAllCmd)
	return cmd
}

func tenantArg(ctx cli.CommandContext) (string, error) {
	args := ctx.Args()
	if len(args) != 1 || args[0] == "" {
		return "", fmt.Errorf("usage: fabriq tenant <verb> <tenant-id>")
	}
	return args[0], nil
}

func catalogFromContext(ctx cli.CommandContext) (catalog.Catalog, func(), error) {
	dsn := ctx.String("catalog-dsn")
	if dsn == "" {
		dsn = os.Getenv("FABRIQ_CATALOG_DSN")
	}
	if dsn == "" {
		return nil, nil, fmt.Errorf("catalog DSN required (--catalog-dsn or FABRIQ_CATALOG_DSN)")
	}
	store, err := postgres.OpenCatalog(ctx.Context(), dsn)
	if err != nil {
		return nil, nil, err
	}
	return store, func() { _ = store.Close() }, nil
}

func provisionerFromContext(ctx cli.CommandContext) (*provision.Provisioner, func(), error) {
	cat, closeFn, err := catalogFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	clusters, err := parseClusterDSNs(ctx)
	if err != nil {
		closeFn()
		return nil, nil, err
	}
	return provision.New(cat, postgres.NewClusterOps(clusters)), closeFn, nil
}

// parseClusterDSNs reads "id=dsn,id=dsn" from --clusters or
// FABRIQ_CLUSTER_DSNS.
func parseClusterDSNs(ctx cli.CommandContext) (map[string]string, error) {
	raw := ctx.String("clusters")
	if raw == "" {
		raw = os.Getenv("FABRIQ_CLUSTER_DSNS")
	}
	if raw == "" {
		return nil, fmt.Errorf(`cluster DSNs required (--clusters "id=dsn,..." or FABRIQ_CLUSTER_DSNS)`)
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		id, dsn, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || id == "" || dsn == "" {
			return nil, fmt.Errorf("malformed cluster entry %q (want id=dsn)", pair)
		}
		out[id] = dsn
	}
	return out, nil
}
