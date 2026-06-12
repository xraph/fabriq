package main

import (
	"fmt"
	"strings"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/adapters/postgres"
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
	cmd := cli.NewCommand("rebuild", "Blue-green projection rebuild for a tenant", func(_ cli.CommandContext) error {
		return cliError("rebuild is not implemented yet (phase 4: graph projection + blue-green rebuild)")
	})
	cmd.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	cmd.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	cmd.AddFlag(cli.NewStringFlag("projection", "p", "graph|search", ""))
	cmd.AddFlag(cli.NewBoolFlag("all-tenants", "", "rebuild every tenant", false))
	return cmd
}

func reconcileCommand() cli.Command {
	cmd := cli.NewCommand("reconcile", "Compare Postgres against projections and re-emit drifted aggregates", func(_ cli.CommandContext) error {
		return cliError("reconcile is not implemented yet (phase 6: reconciler)")
	})
	cmd.AddFlag(cli.NewStringFlag("dsn", "", "Postgres DSN (or FABRIQ_POSTGRES_DSN)", ""))
	cmd.AddFlag(cli.NewStringFlag("tenant", "t", "tenant id", ""))
	cmd.AddFlag(cli.NewBoolFlag("repair", "", "re-emit synthetic events for drifted aggregates", false))
	return cmd
}
