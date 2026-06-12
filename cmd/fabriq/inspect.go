package main

import (
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

func rebuildCommand() cli.Command {
	cmd := cli.NewCommand("rebuild", "Blue-green projection rebuild for a tenant", func(ctx cli.CommandContext) error {
		dsn, ok := dsnFromContext(ctx)
		if !ok {
			return errMissingDSN
		}
		proj := ctx.String("projection")
		if proj != "graph" && proj != "search" {
			return cliError("--projection must be graph or search")
		}
		cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: dsn}}
		switch proj {
		case "graph":
			addr := ctx.String("falkordb")
			if addr == "" {
				addr = os.Getenv("FABRIQ_FALKORDB_ADDR")
			}
			if addr == "" {
				return cliError("--falkordb (or FABRIQ_FALKORDB_ADDR) is required for graph rebuilds")
			}
			cfg.FalkorDB.Addr = addr
		case "search":
			addrs := ctx.String("elasticsearch")
			if addrs == "" {
				addrs = os.Getenv("FABRIQ_ELASTICSEARCH_ADDRS")
			}
			if addrs == "" {
				return cliError("--elasticsearch (or FABRIQ_ELASTICSEARCH_ADDRS) is required for search rebuilds")
			}
			cfg.Elasticsearch.Addrs = strings.Split(addrs, ",")
		}

		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
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

		tenants := []string{ctx.String("tenant")}
		if ctx.Bool("all-tenants") {
			tenants, err = stores.Postgres.ProjectionState().Tenants(ctx.Context())
			if err != nil {
				return err
			}
		} else if tenants[0] == "" {
			return cliError("--tenant is required (or pass --all-tenants)")
		}

		for _, tenantID := range tenants {
			oldTarget, newTarget, err := rebuilder.Rebuild(ctx.Context(), tenantID)
			if err != nil {
				return err
			}
			ctx.Success(fmt.Sprintf("tenant %s: built %s (was %q), status=soaking", tenantID, newTarget, oldTarget))
			if ctx.Bool("drop-old") {
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
			} else {
				ctx.Info(fmt.Sprintf("soak, then: fabriq rebuild finalize --tenant %s --old %s", tenantID, oldTarget))
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
		falkorAddr := ctx.String("falkordb")
		if falkorAddr == "" {
			falkorAddr = os.Getenv("FABRIQ_FALKORDB_ADDR")
		}
		tenantID := ctx.String("tenant")
		if tenantID == "" || falkorAddr == "" {
			return cliError("--tenant and --falkordb are required")
		}
		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
			return err
		}
		_, stores, err := fabriq.Open(ctx.Context(), r, fabriq.Config{
			Postgres: fabriq.PostgresConfig{DSN: dsn},
			FalkorDB: fabriq.FalkorDBConfig{Addr: falkorAddr},
		})
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
		cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: dsn}}
		if addr := orEnv(ctx.String("falkordb"), "FABRIQ_FALKORDB_ADDR"); addr != "" {
			cfg.FalkorDB.Addr = addr
		}
		if addrs := orEnv(ctx.String("elasticsearch"), "FABRIQ_ELASTICSEARCH_ADDRS"); addrs != "" {
			cfg.Elasticsearch.Addrs = strings.Split(addrs, ",")
		}
		if cfg.FalkorDB.Addr == "" && len(cfg.Elasticsearch.Addrs) == 0 {
			return cliError("nothing to reconcile: pass --falkordb and/or --elasticsearch")
		}

		r := registry.New()
		if err := domain.RegisterAll(r); err != nil {
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

func orEnv(v, env string) string {
	if v != "" {
		return v
	}
	return os.Getenv(env)
}
