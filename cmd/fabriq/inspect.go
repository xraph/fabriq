package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

func inspectCommand() cli.Command {
	cmd := cli.NewCommand("inspect", "Inspect the registry and projection state", func(ctx cli.CommandContext) error {
		ctx.Println("usage: fabriq inspect registry|state")
		return nil
	})

	reg := cli.NewCommand("registry", "Dump registered entity specs", func(ctx cli.CommandContext) error {
		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
			return err
		}
		if err := r.Validate(); err != nil {
			return err
		}
		for _, ent := range r.All() {
			s := ent.Spec
			ctx.Println(fmt.Sprintf("%-8s kind=%-9s table=%-10s graph=%-6s search=%q",
				s.Name, s.Kind, ent.Binding.Table, s.GraphNode, s.Search.Index))
			for _, e := range s.Edges {
				ctx.Println(fmt.Sprintf("  edge %s -[%s]-> %s (field %s)", s.Name, e.Rel, e.Target, e.Field))
			}
			scopes := make([]string, 0, len(s.Subscribe))
			for _, sc := range s.Subscribe {
				scopes = append(scopes, sc.Name)
			}
			ctx.Println("  scopes: " + strings.Join(scopes, ", "))
		}
		return nil
	})

	state := cli.NewCommand("state", "Show projection state for a tenant", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		tenantID := ctx.String("tenant")
		if tenantID == "" {
			return cliError("--tenant is required")
		}
		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
			return err
		}
		a, err := postgres.Open(ctx.Context(), dsn, r)
		if err != nil {
			return err
		}
		defer func() { _ = a.Close() }()

		for _, proj := range []string{"graph", "search"} {
			s, err := a.ProjectionState().Get(ctx.Context(), tenantID, proj)
			if err != nil {
				return err
			}
			ctx.Println(fmt.Sprintf("%-7s model_v%-3d status=%-9s target=%-30s stream=%s",
				s.Projection, s.ModelVersion, s.Status, s.TargetName, s.EventVersion))
		}
		return nil
	})
	state.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	state.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))

	_ = cmd.AddSubcommand(reg)
	_ = cmd.AddSubcommand(state)
	return cmd
}

// rebuildCommand is `fabriq rebuild`. It exits 0 when every selected tenant
// rebuilt (including --all-tenants with none to rebuild), 1 on a runtime
// error and 2 on a usage error. It stops at the first tenant that fails:
// the tenants before it stay rebuilt, and a rerun is safe because each run
// builds a fresh target.
func rebuildCommand() cli.Command {
	cmd := cli.NewCommand("rebuild", "Blue-green projection rebuild for a tenant", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		proj := ctx.String("projection")
		if proj != "graph" && proj != "search" {
			return usageError("--projection must be graph or search")
		}
		allTenants := ctx.Bool("all-tenants")
		tenantID := ctx.String("tenant")
		if !allTenants && tenantID == "" {
			return usageError("--tenant is required (or pass --all-tenants)")
		}
		cfg := operatorStoreConfig(ctx, dsn)
		switch proj {
		case "graph":
			if cfg.FalkorDB.Addr == "" {
				return usageError("--falkordb (or FABRIQ_FALKORDB_ADDR) is required for graph rebuilds")
			}
		case "search":
			if len(cfg.Elasticsearch.Addrs) == 0 {
				return usageError("--elasticsearch (or FABRIQ_ELASTICSEARCH_ADDRS) is required for search rebuilds")
			}
		}
		r, err := operatorRegistry(os.Getenv, &cfg)
		if err != nil {
			return err
		}

		_, stores, err := fabriq.Open(ctx.Context(), r, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = stores.Close() }()

		var rebuilder *projection.Rebuilder
		if proj == "graph" {
			rebuilder, err = stores.GraphRebuilder(r)
		} else {
			rebuilder, err = stores.SearchRebuilder(r)
		}
		if err != nil {
			return err
		}

		tenants := []string{tenantID}
		if allTenants {
			tenants, err = stores.AllTenants(ctx.Context())
			if err != nil {
				return err
			}
			if len(tenants) == 0 {
				ctx.Info("no tenants to rebuild")
				return nil
			}
		}

		for _, tid := range tenants {
			if err := rebuildTenant(ctx, rebuilder, proj, tid); err != nil {
				return fmt.Errorf("tenant %s: %w", tid, err)
			}
		}
		return nil
	})
	cmd.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	cmd.AddFlag(cli.NewStringFlag("falkordb", "", "FalkorDB address (or FABRIQ_FALKORDB_ADDR)", ""))
	cmd.AddFlag(cli.NewStringFlag("elasticsearch", "", "Elasticsearch addrs, comma-separated (or FABRIQ_ELASTICSEARCH_ADDRS)", ""))
	cmd.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	cmd.AddFlag(cli.NewStringFlag("projection", "p", "graph|search", "graph"))
	cmd.AddFlag(cli.NewBoolFlag("all-tenants", "", "rebuild every tenant", false))
	cmd.AddFlag(cli.NewBoolFlag("drop-old", "", "drop the old target immediately instead of soaking", false))

	finalize := cli.NewCommand("finalize", "End the soak: drop the old target, mark live", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		cfg := operatorStoreConfig(ctx, dsn)
		tenantID := ctx.String("tenant")
		if tenantID == "" || cfg.FalkorDB.Addr == "" {
			return usageError("--tenant and --falkordb (or FABRIQ_FALKORDB_ADDR) are required")
		}
		r, err := operatorRegistry(os.Getenv, &cfg)
		if err != nil {
			return err
		}
		_, stores, err := fabriq.Open(ctx.Context(), r, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = stores.Close() }()
		rebuilder, err := stores.GraphRebuilder(r)
		if err != nil {
			return err
		}
		if err := rebuilder.Finalize(ctx.Context(), tenantID, ctx.String("old")); err != nil {
			return err
		}
		ctx.Success("finalized: status=live")
		return nil
	})
	finalize.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	finalize.AddFlag(cli.NewStringFlag("falkordb", "", "FalkorDB address (or FABRIQ_FALKORDB_ADDR)", ""))
	finalize.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	finalize.AddFlag(cli.NewStringFlag("old", "", "old target to drop", ""))
	_ = cmd.AddSubcommand(finalize)
	return cmd
}

func reconcileCommand() cli.Command {
	cmd := cli.NewCommand("reconcile", "Compare Postgres against projections and re-emit drifted aggregates", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		tenantID := ctx.String("tenant")
		if tenantID == "" {
			return cliError("--tenant is required")
		}
		cfg := operatorStoreConfig(ctx, dsn)
		if cfg.FalkorDB.Addr == "" && len(cfg.Elasticsearch.Addrs) == 0 {
			return cliError("nothing to reconcile: pass --falkordb and/or --elasticsearch")
		}
		r, err := operatorRegistry(os.Getenv, &cfg)
		if err != nil {
			return err
		}
		_, stores, err := fabriq.Open(ctx.Context(), r, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = stores.Close() }()

		repair := ctx.Bool("repair")
		run := func(name string, rec *projection.Reconciler) error {
			drifts, err := rec.Reconcile(ctx.Context(), tenantID, repair)
			if err != nil {
				return err
			}
			if len(drifts) == 0 {
				ctx.Success(fmt.Sprintf("%s: no drift", name))
				return nil
			}
			for _, d := range drifts {
				ctx.Warning(fmt.Sprintf("%s drift: %s/%s truth=v%d projected=v%d",
					name, d.Entity, d.AggID, d.TruthVersion, d.ProjectedVersion))
			}
			if repair {
				ctx.Success(fmt.Sprintf("%s: %d aggregates re-emitted through the outbox", name, len(drifts)))
			} else {
				ctx.Info(fmt.Sprintf("%s: %d drifted (dry run; pass --repair)", name, len(drifts)))
			}
			return nil
		}

		if stores.Falkor != nil {
			rec, err := stores.GraphReconciler(r)
			if err != nil {
				return err
			}
			if err := run("graph", rec); err != nil {
				return err
			}
		}
		if stores.Elastic != nil {
			rec, err := stores.SearchReconciler(r)
			if err != nil {
				return err
			}
			if err := run("search", rec); err != nil {
				return err
			}
		}
		return nil
	})
	cmd.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	cmd.AddFlag(cli.NewStringFlag("falkordb", "", "FalkorDB address (or FABRIQ_FALKORDB_ADDR)", ""))
	cmd.AddFlag(cli.NewStringFlag("elasticsearch", "", "Elasticsearch addrs (or FABRIQ_ELASTICSEARCH_ADDRS)", ""))
	cmd.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	cmd.AddFlag(cli.NewBoolFlag("repair", "", "re-emit drifted aggregates through the outbox", false))
	return cmd
}

// rebuildTenant rebuilds one tenant's projection into a fresh target. With
// --drop-old it also drops the old target and marks the tenant live; without
// it the tenant stays soaking and the command prints the finalize step.
func rebuildTenant(ctx cli.CommandContext, rebuilder *projection.Rebuilder, proj, tenantID string) error {
	oldTarget, newTarget, err := rebuilder.Rebuild(ctx.Context(), tenantID)
	if err != nil {
		return err
	}
	ctx.Success(fmt.Sprintf("tenant %s: built %s (was %q), status=soaking", tenantID, newTarget, oldTarget))
	if !ctx.Bool("drop-old") {
		ctx.Info(fmt.Sprintf("soak, then: fabriq rebuild finalize --tenant %s --old %s", tenantID, oldTarget))
		return nil
	}
	if oldTarget == "" {
		if proj == "graph" {
			oldTarget = "tenant_" + tenantID // the unversioned initial live graph
		} else {
			oldTarget = "v1" // the initial live search model
		}
	}
	if err := rebuilder.Finalize(ctx.Context(), tenantID, oldTarget); err != nil {
		return err
	}
	ctx.Success(fmt.Sprintf("tenant %s: dropped %s, status=live", tenantID, oldTarget))
	return nil
}

// usageError is a problem with an operator command's flags or environment.
// It exits 2, where a runtime failure exits 1.
func usageError(msg string) error { return cli.NewError(msg, exitUsage) }

// operatorStoreConfig is the store config an operator command opens with:
// the engine addresses and credentials from storeConfigFromEnv, the DSN, and
// then the --falkordb and --elasticsearch flags over the env addresses.
func operatorStoreConfig(ctx cli.CommandContext, dsn string) fabriq.Config {
	cfg := storeConfigFromEnv(os.Getenv)
	cfg.Postgres.DSN = dsn
	if addr := ctx.String("falkordb"); addr != "" {
		cfg.FalkorDB.Addr = addr
	}
	if addrs := splitAddrs(ctx.String("elasticsearch")); len(addrs) > 0 {
		cfg.Elasticsearch.Addrs = addrs
	}
	return cfg
}

// operatorRegistry builds the registry an operator command opens fabriq with
// and sets cfg.Dynamic.Persist, the way serve does: FABRIQ_DOMAIN_PACK picks
// the code-registered entities and FABRIQ_DYNAMIC_PERSIST (default true)
// loads the runtime-defined ones. A bad value in either is a usage error; a
// pack that fails to register is a runtime one. Catalog mode refuses
// dynamic.persist, so persistence stays off with a catalog config.
func operatorRegistry(getenv func(string) string, cfg *fabriq.Config) (*registry.Registry, error) {
	persist, err := envBool(getenv, "FABRIQ_DYNAMIC_PERSIST", true)
	if err != nil {
		return nil, usageError(err.Error())
	}
	cfg.Dynamic.Persist = persist && !cfg.Catalog.Enabled()

	r := registry.New()
	if err := registerDomainPack(r, getenv("FABRIQ_DOMAIN_PACK")); err != nil {
		if errors.Is(err, errUnknownDomainPack) {
			return nil, usageError(err.Error())
		}
		return nil, err
	}
	return r, nil
}
