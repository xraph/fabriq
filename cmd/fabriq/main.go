// Command fabriq is the data fabric's single binary: a Forge app wrapped
// in a CLI (forge/cli RunApp). The same image runs the long-lived worker
// and the one-shot operator commands, sharing one wiring path.
//
//	fabriq                        serve (default): run the background plane
//	fabriq serve|start|run        same, explicit
//	fabriq migrate up|down|status grove migrations (advisory-locked)
//	fabriq inspect registry|state dump entity specs / projection state
//	fabriq rebuild                blue-green projection rebuild
//	fabriq reconcile              drift reconciliation
//	fabriq analytics backfill     replay tenant snapshots into the analytics sink
//	fabriq info|health|extensions Forge built-ins
//
// The worker (serve) is a Forge RunnableExtension: leader-elected outbox
// relay, projection consumers, reconciler, document plane. With
// FABRIQ_ADMIN_API=true serve also mounts the admin API under /admin behind
// API-key auth. fabriq's own probes are /_/livez and /_/readyz (ready only
// once the facade is open and the stores answer, with no grace period);
// forge adds /_/health and /_/metrics, fabriq adds /metrics.
//
// Configuration is environment-first (the deployment injects a secret +
// configmap); --dsn overrides FABRIQ_POSTGRES_DSN for ad-hoc operator use.
//
//	FABRIQ_POSTGRES_DSN          (required to serve)
//	FABRIQ_REDIS_ADDR            (required to serve)
//	FABRIQ_FALKORDB_ADDR         (optional: graph projection)
//	FABRIQ_ELASTICSEARCH_ADDRS   (optional, comma-separated: search projection)
//	FABRIQ_HTTP_ADDR             (default :8081)
//	FABRIQ_RECONCILE_INTERVAL    (Go duration, default 5m; "0" disables)
//	FABRIQ_DOMAIN_PACK           (example (default) or none)
//	FABRIQ_DYNAMIC_PERSIST       (default true: persist and reload runtime-defined entity types)
//	FABRIQ_DYNAMIC_SYNC_INTERVAL (Go duration, default 15s: pick up types other processes define)
//	FABRIQ_ADMIN_API             (default false: mount the admin API with key auth)
//	FABRIQ_ADMIN_KEY             (required when FABRIQ_ADMIN_API=true: bootstrap key, fq_ + 32 or more letters or digits)
//	FABRIQ_ADMIN_LOGIN_USER      (optional, with FABRIQ_ADMIN_LOGIN_PASSWORD: enable POST /admin/login)
//	FABRIQ_ADMIN_LOGIN_PASSWORD  (optional, with FABRIQ_ADMIN_LOGIN_USER)
//	FABRIQ_ALLOW_RLS_BYPASS      (default false; development only: let the admin API serve as a superuser or BYPASSRLS role)
//
// A malformed serve setting (a bad bool or duration, an unknown pack, half a
// login pair) fails every command, not just serve, because the config is
// wrong. FABRIQ_ADMIN_KEY is checked only when the admin API starts, so migrate and
// the other operator commands never need it.
package main

import (
	"os"

	"github.com/xraph/forge"
	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq/forgeext"
)

// version is the build version, stamped by goreleaser via
// -ldflags "-X main.version=...". It defaults to "dev" for local builds.
var version = "dev"

func main() {
	cli.RunApp(setup, runAppOptions()...)
}

// runAppOptions are the forge/cli options main runs with.
func runAppOptions() []cli.RunAppOption {
	return []cli.RunAppOption{
		cli.WithCLIName("fabriq"),
		cli.WithCLIVersion(version),
		cli.WithCLIDescription("fabriq data fabric: worker + operations"),
		cli.WithGlobalFlags(
			cli.NewStringFlag("dsn", "", "Postgres DSN (overrides FABRIQ_POSTGRES_DSN)", ""),
		),
		// fabriq's migrate runs grove's orchestrator off a bare DSN (no
		// store fan-out, no app role DDL). Forge's built-in migrate would
		// app.Start() every store first, so disable it and ship ours.
		cli.WithDisableMigrationCommands(),
		cli.WithExtraCommands(
			migrateCommand(), inspectCommand(), rebuildCommand(), reconcileCommand(), tenantCommand(), analyticsCommand(),
		),
	}
}

// setup constructs the serve Forge app. It is resolved eagerly for every
// command (forge/cli builds the app before any handler runs), so it must
// stay I/O-free and must not fail when the worker's stores are unset: the
// operator commands open their own stores and never serve. Store
// validation and connection, and the admin API's key check, happen in the
// extensions' Register and Start, which only app.Start (the serve path)
// triggers. A malformed serve setting does fail here, for every command.
func setup(_ cli.CommandContext) (forge.App, error) {
	s, err := loadServeSettings(os.Getenv)
	if err != nil {
		return nil, err
	}
	app := newServeForgeApp(s)

	// Load the datastore config from forge's config manager. Config is loaded
	// here (not in Start) so that the extension receives the top-level key
	// contract (FABRIQ_POSTGRES_DSN, etc.) rather than the extensions.fabriq.*
	// prefix that a host-app would use.
	cfg := forgeext.LoadConfig(app.Config(), "")
	if err := registerServe(app, s, cfg); err != nil {
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
