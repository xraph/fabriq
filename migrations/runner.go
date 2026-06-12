package migrations

import (
	"context"
	"fmt"

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
