// Command fabriq is the data-fabric operations CLI, built on forge/cli:
//
//	fabriq migrate up|down|status   — run grove migrations (advisory-locked)
//	fabriq inspect registry|state   — dump entity specs / projection state
//	fabriq rebuild                  — blue-green projection rebuild (phase 4)
//	fabriq reconcile                — drift reconciliation (phase 6)
//
// Configuration: --dsn flag or FABRIQ_POSTGRES_DSN.
package main

import (
	"os"

	"github.com/xraph/forge/cli"
)

func main() {
	app := cli.New(cli.Config{
		Name:        "fabriq",
		Version:     "0.1.0",
		Description: "TWINOS data fabric operations",
	})

	for _, cmd := range []cli.Command{
		migrateCommand(), inspectCommand(), rebuildCommand(), reconcileCommand(),
	} {
		if err := app.AddCommand(cmd); err != nil {
			panic(err)
		}
	}

	if err := app.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

// dsnFromContext resolves the Postgres DSN from --dsn or the environment.
func dsnFromContext(ctx cli.CommandContext) (string, bool) {
	dsn := ctx.String("dsn")
	if dsn == "" {
		dsn = os.Getenv("FABRIQ_POSTGRES_DSN")
	}
	if dsn == "" {
		ctx.Error(errMissingDSN)
		return "", false
	}
	return dsn, true
}

type cliError string

func (e cliError) Error() string { return string(e) }

const errMissingDSN = cliError("postgres DSN required: pass --dsn or set FABRIQ_POSTGRES_DSN")
