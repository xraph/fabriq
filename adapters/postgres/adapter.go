// Package postgres is fabriq's Postgres adapter, built on grove's pg
// driver. It implements the relational, timeseries, vector and command
// store ports against the source of truth.
//
// Tenancy is enforced in layers:
//
//  1. Structurally: every operation runs inside a transaction stamped with
//     SET LOCAL app.tenant_id (set_config(..., true)), and every generated
//     query carries an explicit tenant predicate where applicable.
//  2. In the database: RLS policies (FORCE) key on that setting, so even
//     raw SQL through the escape hatch cannot cross tenants.
//  3. Backstop: a grove pre-query hook DENIES any pool-path access to a
//     tenant table — in this architecture such access is always a bug —
//     returning ErrTenantHookTripped and counting the trip.
//
// (grove's PgTx builders bypass the hook engine, which is why the hook
// guards the pool path while RLS guards the transaction path — see
// docs/decisions/0002-tenancy-layers.md.)
package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// Adapter implements command.Store, query.RelationalQuerier,
// query.TSQuerier and query.VectorQuerier on grove/pgdriver.
type Adapter struct {
	gdb      *grove.DB
	pg       *pgdriver.PgDB
	dsn      string
	reg      *registry.Registry
	backstop *tenantBackstop
	state    *StateRepo
}

var _ query.RelationalQuerier = (*Adapter)(nil)

// Option configures Open.
type Option func(*openConfig)

type openConfig struct {
	poolSize      int
	guardedTables []string
}

// WithPoolSize sets the pgx pool size.
func WithPoolSize(n int) Option {
	return func(c *openConfig) { c.poolSize = n }
}

// WithGuardedTables adds tables to the tenant guard beyond the registry's
// (e.g. the telemetry hypertable, which has no RLS because Timescale
// columnstore forbids it).
func WithGuardedTables(tables ...string) Option {
	return func(c *openConfig) { c.guardedTables = append(c.guardedTables, tables...) }
}

// Open connects to Postgres and wires the tenant backstop.
func Open(ctx context.Context, dsn string, reg *registry.Registry, opts ...Option) (*Adapter, error) {
	if reg == nil {
		return nil, fmt.Errorf("fabriq: postgres adapter needs the registry")
	}
	cfg := openConfig{poolSize: 16}
	for _, opt := range opts {
		opt(&cfg)
	}

	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn, driver.WithPoolSize(cfg.poolSize)); err != nil {
		return nil, fmt.Errorf("fabriq: open postgres: %w", err)
	}
	gdb, err := grove.Open(pg)
	if err != nil {
		_ = pg.Close()
		return nil, err
	}

	backstop := newTenantBackstop(reg, cfg.guardedTables)
	engine := gdb.Hooks()
	engine.AddHook(backstop)
	pg.SetHooks(engine) // grove.Open does NOT propagate; explicit by design

	a := &Adapter{gdb: gdb, pg: pg, dsn: dsn, reg: reg, backstop: backstop}
	a.state = &StateRepo{pg: pg}
	return a, nil
}

// Close releases the connection pool.
func (a *Adapter) Close() error { return a.gdb.Close() }

// Grove exposes the grove handle (hook-guarded pool path) for advanced
// embedding. Tenant tables are NOT reachable through it — the backstop
// denies them; use the fabric ports.
func (a *Adapter) Grove() *grove.DB { return a.gdb }

// Driver exposes the typed pg driver for worker-plane components living in
// this module (relay, leader, migrations CLI). Never hand it to
// application code.
func (a *Adapter) Driver() *pgdriver.PgDB { return a.pg }

// BackstopTrips reports how many times the tenant backstop fired.
func (a *Adapter) BackstopTrips() int64 { return a.backstop.trips.Load() }

// ProjectionState returns the projection bookkeeping repo.
func (a *Adapter) ProjectionState() *StateRepo { return a.state }

// inTenantTx runs fn inside a transaction stamped with the context tenant.
func (a *Adapter) inTenantTx(ctx context.Context, fn func(tx *pgdriver.PgTx) error) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	ptx, err := a.pg.BeginTxQuery(ctx, nil)
	if err != nil {
		return fmt.Errorf("fabriq: begin tx: %w", err)
	}
	defer func() { _ = ptx.Rollback() }()

	if _, err := ptx.NewRaw(`SELECT set_config('app.tenant_id', $1, true)`, tid).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: stamp tenant: %w", err)
	}
	if err := fn(ptx); err != nil {
		return err
	}
	if err := ptx.Commit(); err != nil {
		return fmt.Errorf("fabriq: commit: %w", err)
	}
	return nil
}

// entity resolves a registry entity and checks the scan target type.
func (a *Adapter) entity(name string) (*registry.Entity, error) {
	ent, ok := a.reg.Get(name)
	if !ok {
		return nil, fmt.Errorf("fabriq: unknown entity %q", name)
	}
	return ent, nil
}

// Get implements query.RelationalQuerier.
func (a *Adapter) Get(ctx context.Context, entity, id string, into any) error {
	ent, err := a.entity(entity)
	if err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		err := tx.NewSelect(into).
			Where(registry.ColumnTenant+" = ?", tid).
			Where(registry.ColumnID+" = ?", id).
			Limit(1).
			Scan(ctx)
		if isNoRows(err) {
			return &fabriqerr.NotFoundError{Entity: ent.Spec.Name, ID: id}
		}
		return err
	})
}

// GetMany implements the batched hydration contract: ONE query, results in
// ids order, missing rows skipped.
func (a *Adapter) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	ent, err := a.entity(entity)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		sql := fmt.Sprintf(
			`SELECT * FROM %s WHERE tenant_id = $1 AND id = ANY($2) ORDER BY array_position($2, id)`,
			quoteIdent(ent.Binding.Table))
		return tx.NewRaw(sql, tid, ids).Scan(ctx, into)
	})
}

// List implements equality-filtered, ordered paging. Filter and order
// columns are validated against the binding — unknown columns are
// rejected, which is also the SQL-injection guard.
func (a *Adapter) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	ent, err := a.entity(entity)
	if err != nil {
		return err
	}
	orderCol, orderDir, err := splitOrder(ent, q.OrderBy)
	if err != nil {
		return err
	}
	for col := range q.Filter {
		if !ent.Binding.HasColumn(col) {
			return fmt.Errorf("fabriq: entity %q has no column %q", entity, col)
		}
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		sel := tx.NewSelect(into).Where(registry.ColumnTenant+" = ?", tid)
		for col, val := range q.Filter {
			sel = sel.Where(quoteIdent(col)+" = ?", val)
		}
		if orderCol != "" {
			sel = sel.OrderExpr(quoteIdent(orderCol) + " " + orderDir)
		} else {
			sel = sel.OrderExpr(quoteIdent(registry.ColumnID) + " ASC")
		}
		if q.Limit > 0 {
			sel = sel.Limit(q.Limit)
		}
		if q.Offset > 0 {
			sel = sel.Offset(q.Offset)
		}
		return sel.Scan(ctx)
	})
}

// Query is the raw SQL escape hatch for reads. It still runs inside a
// tenant-stamped transaction, so RLS contains it; tables outside RLS
// (guarded tables) additionally require a literal tenant_id reference.
func (a *Adapter) Query(ctx context.Context, into any, sql string, args ...any) error {
	if err := a.backstop.guardRawSQL(sql); err != nil {
		return err
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(sql, args...).Scan(ctx, into)
	})
}

func splitOrder(ent *registry.Entity, orderBy string) (col, dir string, err error) {
	if orderBy == "" {
		return "", "", nil
	}
	parts := strings.Fields(orderBy)
	col = parts[0]
	dir = "ASC"
	if len(parts) > 1 {
		switch strings.ToUpper(parts[1]) {
		case "ASC", "DESC":
			dir = strings.ToUpper(parts[1])
		default:
			return "", "", fmt.Errorf("fabriq: invalid order direction %q", parts[1])
		}
	}
	if len(parts) > 2 || !ent.Binding.HasColumn(col) {
		return "", "", fmt.Errorf("fabriq: invalid order column %q for entity %q", orderBy, ent.Spec.Name)
	}
	return col, dir, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, ``) + `"`
}

func isNoRows(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows")
}
