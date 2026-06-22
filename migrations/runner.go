package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

// OpenOrchestrator dials Postgres and returns a ready orchestrator plus a
// close function. It exists so the CLI (and worker bootstrap) can run
// migrations without importing grove drivers directly — driver imports
// stay fenced to adapters/, fabriqtest/ and this package.
func OpenOrchestrator(ctx context.Context, dsn string) (*migrate.Orchestrator, func() error, error) {
	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn); err != nil {
		return nil, nil, fmt.Errorf("fabriq: open postgres for migrations: %w", err)
	}
	orch, err := NewOrchestrator(pg)
	if err != nil {
		_ = pg.Close()
		return nil, nil, err
	}
	return orch, pg.Close, nil
}

// NewOrchestratorFromGrove builds an orchestrator on an already-open grove.DB
// borrowed from a host DI container — the migration counterpart to the forge
// extension's grove fallback (mirrors how xraph/authsome shares the host
// grove). The grove MUST be pg-backed. The returned close func is a no-op: the
// host owns the connection lifecycle, so migrations must not tear it down.
func NewOrchestratorFromGrove(gdb *grove.DB) (*migrate.Orchestrator, func() error, error) {
	if gdb == nil {
		return nil, nil, fmt.Errorf("fabriq: migrations need a non-nil grove.DB")
	}
	if _, ok := gdb.Driver().(*pgdriver.PgDB); !ok {
		return nil, nil, fmt.Errorf("fabriq: migrations need a pg-backed grove.DB, got %q", gdb.Driver().Name())
	}
	orch, err := NewOrchestrator(gdb.Driver())
	if err != nil {
		return nil, nil, err
	}
	return orch, func() error { return nil }, nil
}

// OpenOrchestratorWith returns a migration orchestrator, preferring an explicit
// DSN (dialing a fresh connection it owns and closes) and otherwise borrowing
// the given grove.DB. It errors when neither is available, so a misconfigured
// caller gets a clear message instead of an "empty dsn" dial failure.
func OpenOrchestratorWith(ctx context.Context, dsn string, gdb *grove.DB) (*migrate.Orchestrator, func() error, error) {
	if dsn != "" {
		return OpenOrchestrator(ctx, dsn)
	}
	if gdb != nil {
		return NewOrchestratorFromGrove(gdb)
	}
	return nil, nil, fmt.Errorf("fabriq: migrations need a Postgres DSN or a grove.DB (set postgres.dsn / shards, or register a *grove.DB in the host container)")
}
