// Command fabriq is the data fabric's single binary: a Forge app wrapped
// in a CLI (forge/cli RunApp). The same image runs the long-lived worker
// and the one-shot operator commands, sharing one wiring path.
//
//	fabriq                    — serve (default): run the background plane
//	fabriq serve|start|run    — same, explicit
//	fabriq migrate up|down|status — grove migrations (advisory-locked)
//	fabriq inspect registry|state — dump entity specs / projection state
//	fabriq rebuild                — blue-green projection rebuild
//	fabriq reconcile              — drift reconciliation
//	fabriq info|health|extensions — Forge built-ins
//
// The worker (serve) is a Forge RunnableExtension: leader-elected outbox
// relay, projection consumers, reconciler, document plane. Health and
// metrics come from Forge: /_/livez, /_/readyz, /_/health, plus /metrics.
//
// Configuration is environment-first (the deployment injects a secret +
// configmap); --dsn overrides FABRIQ_POSTGRES_DSN for ad-hoc operator use.
//
//	FABRIQ_POSTGRES_DSN        (required to serve)
//	FABRIQ_REDIS_ADDR          (required to serve)
//	FABRIQ_FALKORDB_ADDR       (optional: graph projection)
//	FABRIQ_ELASTICSEARCH_ADDRS (optional, comma-separated: search projection)
//	FABRIQ_HTTP_ADDR           (default :8081)
//	FABRIQ_RECONCILE_INTERVAL  (Go duration; "0" disables)
package main

import (
	"os"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/forgeext"
)

// version is the build version, stamped by goreleaser via
// -ldflags "-X main.version=...". It defaults to "dev" for local builds.
var version = "dev"

func main() {
	cli.RunApp(setup,
		cli.WithCLIName("fabriq"),
		cli.WithCLIVersion(version),
		cli.WithCLIDescription("fabriq data fabric: worker + operations"),
		cli.WithGlobalFlags(
			cli.NewStringFlag("dsn", "", "Postgres DSN (overrides FABRIQ_POSTGRES_DSN)", ""),
		),
		// fabriq's migrate runs grove's orchestrator off a bare DSN (no
		// store fan-out, no app role DDL). Forge's built-in migrate would
		// app.Start() every store first — disable it and ship ours.
		cli.WithDisableMigrationCommands(),
		cli.WithExtraCommands(
			migrateCommand(), inspectCommand(), rebuildCommand(), reconcileCommand(), tenantCommand(),
		),
	)
}

// setup constructs the worker Forge app. It is resolved eagerly for every
// command (forge/cli builds the app before any handler runs), so it must
// stay I/O-free and must not fail when the worker's stores are unset — the
// operator commands open their own stores and never serve. Store
// validation and connection happen in the worker extension's Start, which
// only the serve path triggers.
func setup(_ cli.CommandContext) (forge.App, error) {
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		return nil, err
	}

	addr := os.Getenv("FABRIQ_HTTP_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	// Forge loads the datastore config: it auto-discovers config.yaml (+
	// config.local.yaml) from the search paths and overlays FABRIQ_*
	// environment variables (env wins). EnvPrefix is pinned to FABRIQ_
	// explicitly so the documented FABRIQ_POSTGRES_DSN-style contract —
	// and the Helm chart that injects it — keeps working regardless of
	// how forge derives a default prefix from the app name.
	app := forge.NewApp(forge.AppConfig{
		Name:                      "fabriq",
		Version:                   version,
		HTTPAddress:               addr,
		EnableConfigAutoDiscovery: true,
		EnableEnvConfig:           true,
		EnvOverridesFile:          true,
		EnvPrefix:                 "FABRIQ_",
		ConfigSearchPaths:         []string{".", "/etc/fabriq"},
	})

	// Load the datastore config from forge's config manager. Config is loaded
	// here (not in Start) so that the extension receives the top-level key
	// contract (FABRIQ_POSTGRES_DSN, etc.) rather than the extensions.fabriq.*
	// prefix that a host-app would use. Store validation (Postgres + Redis
	// required) runs in the extension's Start, which is only reached on serve.
	cm := app.Config()
	cfg := forgeext.LoadConfig(cm, "") // top-level keys + FABRIQ_* — unchanged serve contract
	interval := 5 * time.Minute
	if raw := os.Getenv("FABRIQ_RECONCILE_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			interval = d // "0" disables
		}
	}
	if err := app.RegisterExtension(forgeext.New(reg,
		forgeext.WithConfig(cfg),
		forgeext.WithWorker(true),
		forgeext.WithReconcileInterval(interval),
	)); err != nil {
		return nil, err
	}
	return app, nil
}

// dsnFromContext resolves the Postgres DSN for an operator command from
// --dsn or the environment, erroring (and printing) when neither is set.
func dsnFromContext(ctx cli.CommandContext) (string, bool) {
	if dsn := dsnFromEnvOrFlag(ctx); dsn != "" {
		return dsn, true
	}
	ctx.Error(errMissingDSN)
	return "", false
}

// dsnFromEnvOrFlag returns --dsn if set, else FABRIQ_POSTGRES_DSN, else "".
// Unlike dsnFromContext it never prints — setup needs a silent, lazy read.
func dsnFromEnvOrFlag(ctx cli.CommandContext) string {
	if dsn := ctx.String("dsn"); dsn != "" {
		return dsn
	}
	return os.Getenv("FABRIQ_POSTGRES_DSN")
}

type cliError string

func (e cliError) Error() string { return string(e) }

const errMissingDSN = cliError("postgres DSN required: pass --dsn or set FABRIQ_POSTGRES_DSN")
