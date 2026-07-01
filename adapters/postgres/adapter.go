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
//  3. Backstop: a grove pre-query/pre-mutation hook observes every query
//     on both paths (grove >= a01144a fires hooks inside transactions
//     too). It allows the stamped transaction path — RLS is the guard
//     there — and DENIES any pool-path access to a tenant table, which
//     in this architecture is always a bug, returning
//     ErrTenantHookTripped and counting the trip. See
//     docs/decisions/0002-tenancy-layers.md.
//
// Dynamic-entity reads (IsDynamic() == true) scan rows into
// *[]map[string]any rather than typed structs. Grove's schema-aware Scan
// only handles struct/slice-of-struct destinations, so dynamic reads use the
// PgTx.QueryRows path: a driver.Rows cursor iterated manually with
// Columns()+Scan into individual any destinations, then assembled into maps.
// Static reads are byte-unchanged.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
	reg      *registry.Registry
	backstop *tenantBackstop
	state    *StateRepo
	// owned reports whether this adapter dialed the grove handle itself. When
	// false the handle was borrowed (e.g. resolved from a host DI container),
	// so Close MUST NOT tear it down — the host owns its lifecycle.
	owned bool
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

	return newAdapter(gdb, pg, reg, cfg, true)
}

// OpenWithGrove builds an Adapter on top of a grove.DB that the caller already
// dialed — the seam the forge extension uses to back fabriq's source of truth
// with a *grove.DB resolved from the host's DI container (mirroring how
// xraph/authsome borrows the shared grove) instead of dialing its own DSN.
//
// The grove MUST be backed by the pg driver (grove + pgdriver): fabriq's
// command/relational/timeseries/vector plane is Postgres-specific. The handle
// is BORROWED — Close is a no-op for it, leaving the host to own the
// connection lifecycle. The tenant backstop is still registered on the shared
// hook engine, so pool-path access to fabriq's tenant tables is denied exactly
// as on a self-dialed adapter (host queries to other tables pass through).
func OpenWithGrove(gdb *grove.DB, reg *registry.Registry, opts ...Option) (*Adapter, error) {
	if gdb == nil {
		return nil, fmt.Errorf("fabriq: postgres adapter needs a non-nil grove.DB")
	}
	if reg == nil {
		return nil, fmt.Errorf("fabriq: postgres adapter needs the registry")
	}
	pg, ok := gdb.Driver().(*pgdriver.PgDB)
	if !ok {
		return nil, fmt.Errorf("fabriq: borrowed grove must be backed by the pg driver, got %q", gdb.Driver().Name())
	}
	cfg := openConfig{poolSize: 16}
	for _, opt := range opts {
		opt(&cfg)
	}
	return newAdapter(gdb, pg, reg, cfg, false)
}

// newAdapter wires the tenant backstop onto the (self-dialed or borrowed)
// grove handle and assembles the Adapter. grove.Open propagates its hook
// engine to the driver (grove >= a01144a); registering on gdb.Hooks() is all
// that is needed for the backstop to fire on driver-built queries.
func newAdapter(gdb *grove.DB, pg *pgdriver.PgDB, reg *registry.Registry, cfg openConfig, owned bool) (*Adapter, error) {
	backstop := newTenantBackstop(reg, cfg.guardedTables)
	gdb.Hooks().AddHook(backstop)

	a := &Adapter{gdb: gdb, pg: pg, reg: reg, backstop: backstop, owned: owned}
	a.state = &StateRepo{pg: pg}
	return a, nil
}

// Close releases the connection pool. For a borrowed grove (OpenWithGrove) it
// is a no-op: the host owns the handle's lifecycle.
func (a *Adapter) Close() error {
	if !a.owned {
		return nil
	}
	return a.gdb.Close()
}

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

// TenantTxRaw opens a tenant-stamped transaction for NON-command components
// (e.g. the CAS index) that need raw SQL under app.tenant_id + app.scope_id.
// It is the only sanctioned way to run blob_cas SQL outside the command plane;
// FORCE RLS isolates the work to the context tenant.
func (a *Adapter) TenantTxRaw(ctx context.Context, fn func(tx *pgdriver.PgTx) error) error {
	return a.inTenantTx(ctx, fn)
}

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
	if _, err := ptx.NewRaw(`SELECT set_config('app.scope_id', $1, true)`, tenant.ScopeOrEmpty(ctx)).Exec(ctx); err != nil {
		return fmt.Errorf("fabriq: stamp scope: %w", err)
	}
	if err := fn(ptx); err != nil {
		return err
	}
	if err := ptx.Commit(); err != nil {
		return fmt.Errorf("fabriq: commit: %w", err)
	}
	return nil
}

// inDynamicTenantTx runs fn inside a tenant-stamped transaction, giving fn
// direct access to a driver.Tx so it can call Query() to obtain driver.Rows
// for map-native scanning. This is the dynamic-entity read path; the grove
// query builder (NewSelect) is not used because it only scans struct types.
func (a *Adapter) inDynamicTenantTx(ctx context.Context, fn func(tid string, tx driver.Tx) error) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	tx, err := a.pg.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fabriq: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tid); err != nil {
		return fmt.Errorf("fabriq: stamp tenant: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.scope_id', $1, true)`, tenant.ScopeOrEmpty(ctx)); err != nil {
		return fmt.Errorf("fabriq: stamp scope: %w", err)
	}
	if err := fn(tid, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
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
	if ent.Binding.IsDynamic() {
		if !ddlValid(ent.Binding.Table) {
			return fmt.Errorf("fabriq: dynamic table name %q failed ddl validation", ent.Binding.Table)
		}
		return a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
			sql := fmt.Sprintf(
				`SELECT * FROM %s WHERE tenant_id = $1 AND id = $2 LIMIT 1`,
				quoteIdent(ent.Binding.Table))
			rows, qerr := tx.Query(ctx, sql, tid, id)
			if qerr != nil {
				return qerr
			}
			maps, serr := scanMaps(rows)
			if serr != nil {
				return serr
			}
			if len(maps) == 0 {
				return &fabriqerr.NotFoundError{Entity: ent.Spec.Name, ID: id}
			}
			return assignMapDest(into, maps[0])
		})
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
	if ent.Binding.IsDynamic() {
		if !ddlValid(ent.Binding.Table) {
			return fmt.Errorf("fabriq: dynamic table name %q failed ddl validation", ent.Binding.Table)
		}
		return a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
			sql := fmt.Sprintf(
				`SELECT * FROM %s WHERE tenant_id = $1 AND id = ANY($2) ORDER BY array_position($2, id)`,
				quoteIdent(ent.Binding.Table))
			rows, qerr := tx.Query(ctx, sql, tid, ids)
			if qerr != nil {
				return qerr
			}
			maps, serr := scanMaps(rows)
			if serr != nil {
				return serr
			}
			return assignMapsDest(into, maps)
		})
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
	orderExpr, err := buildOrderExpr(ent, q.OrderBy)
	if err != nil {
		return err
	}
	if err := query.ValidateConds(q.Where, ent.Binding.HasColumn); err != nil {
		return err
	}
	if ent.Binding.IsDynamic() {
		// Build parameterized SQL for the dynamic entity. The table name and
		// order column are ddlValid-checked identifiers that are quoted and
		// interpolated; all filter values travel as $N bound parameters.
		if !ddlValid(ent.Binding.Table) {
			return fmt.Errorf("fabriq: dynamic table name %q failed ddl validation", ent.Binding.Table)
		}
		var sb strings.Builder
		var sqlArgs []any
		argN := 1
		return a.inDynamicTenantTx(ctx, func(tid string, tx driver.Tx) error {
			// Reset sb and args for each call (closure captures them by reference
			// but inDynamicTenantTx only calls the closure once).
			sb.Reset()
			sqlArgs = sqlArgs[:0]
			argN = 1

			fmt.Fprintf(&sb, `SELECT * FROM %s WHERE %s = $%d`,
				quoteIdent(ent.Binding.Table), quoteIdent(registry.ColumnTenant), argN)
			sqlArgs = append(sqlArgs, tid)
			argN++

			for _, c := range q.Where {
				frag, fargs, cerr := condSQLPositional(c, &argN)
				if cerr != nil {
					return cerr
				}
				sb.WriteString(` AND `)
				sb.WriteString(frag)
				sqlArgs = append(sqlArgs, fargs...)
			}

			if orderExpr != "" {
				fmt.Fprintf(&sb, ` ORDER BY %s`, orderExpr)
			} else {
				fmt.Fprintf(&sb, ` ORDER BY %s ASC`, quoteIdent(registry.ColumnID))
			}
			if q.Limit > 0 {
				fmt.Fprintf(&sb, ` LIMIT %d`, q.Limit)
			}
			if q.Offset > 0 {
				fmt.Fprintf(&sb, ` OFFSET %d`, q.Offset)
			}

			rows, qerr := tx.Query(ctx, sb.String(), sqlArgs...)
			if qerr != nil {
				return qerr
			}
			maps, serr := scanMaps(rows)
			if serr != nil {
				return serr
			}
			return assignMapsDest(into, maps)
		})
	}
	return a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		tid, _ := tenant.FromContext(ctx)
		sel := tx.NewSelect(into).Where(registry.ColumnTenant+" = ?", tid)
		for _, c := range q.Where {
			frag, args, cerr := condSQL(c)
			if cerr != nil {
				return cerr
			}
			sel = sel.Where(frag, args...)
		}
		if orderExpr != "" {
			sel = sel.OrderExpr(orderExpr)
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

// scanMaps iterates a driver.Rows cursor and assembles each row into a
// map[string]any keyed by column name. It closes the rows on return.
func scanMaps(rows driver.Rows) ([]map[string]any, error) {
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("fabriq: scanMaps columns: %w", err)
	}

	var out []map[string]any
	for rows.Next() {
		// Allocate one *any per column so Scan can write into it.
		ptrs := make([]any, len(cols))
		vals := make([]any, len(cols))
		for i := range cols {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("fabriq: scanMaps scan: %w", err)
		}
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			m[col] = vals[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fabriq: scanMaps rows.Err: %w", err)
	}
	return out, nil
}

// maxRawQueryRows caps the admin raw-query result so one query cannot stream an
// unbounded number of rows into memory. Hitting it flags truncation.
const maxRawQueryRows = 2000

// scanMapsCapped is scanMaps with a hard row cap. It returns the column list in
// result order and whether the cap was hit (truncated). It closes rows.
func scanMapsCapped(rows driver.Rows, max int) (out []map[string]any, cols []string, truncated bool, err error) {
	defer func() { _ = rows.Close() }()
	cols, err = rows.Columns()
	if err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: scanMapsCapped columns: %w", err)
	}
	for rows.Next() {
		if len(out) >= max {
			truncated = true
			break
		}
		ptrs := make([]any, len(cols))
		vals := make([]any, len(cols))
		for i := range cols {
			ptrs[i] = &vals[i]
		}
		if serr := rows.Scan(ptrs...); serr != nil {
			return nil, nil, false, fmt.Errorf("fabriq: scanMapsCapped scan: %w", serr)
		}
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			m[col] = vals[i]
		}
		out = append(out, m)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, nil, false, fmt.Errorf("fabriq: scanMapsCapped rows.Err: %w", rerr)
	}
	return out, cols, truncated, nil
}

// RawQueryTimeout bounds a single QueryDynamicReadOnly call via statement_timeout.
// Exported so tests can shrink it; production uses the 15s default.
var RawQueryTimeout = 15 * time.Second

// classifyQueryErr maps a cancelled / timed-out query to fabriqerr.ErrQueryTimeout
// — the statement_timeout surfaces as pg SQLSTATE 57014, a context deadline as
// context.DeadlineExceeded — and leaves every other error (bad SQL, etc.) as-is.
func classifyQueryErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", fabriqerr.ErrQueryTimeout, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "57014" {
		return fmt.Errorf("%w: %v", fabriqerr.ErrQueryTimeout, err)
	}
	return err
}

// QueryDynamicReadOnly runs arbitrary read-only SQL for the request tenant and
// returns dynamic rows plus their column order. It runs inside a READ ONLY,
// tenant-stamped transaction, so Postgres itself rejects any write/DDL and RLS
// contains the reads; the backstop still guards against touching a non-RLS
// table without a tenant_id predicate. A 15s statement_timeout and a row cap
// bound cost.
func (a *Adapter) QueryDynamicReadOnly(ctx context.Context, sql string, args ...any) ([]map[string]any, []string, bool, error) {
	if gerr := a.backstop.guardRawSQL(sql); gerr != nil {
		return nil, nil, false, gerr
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	tx, err := a.pg.BeginTx(ctx, &driver.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: begin ro tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// SET and set_config are permitted in a read-only transaction; only data
	// writes are blocked.
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tid); err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: stamp tenant: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.scope_id', $1, true)`, tenant.ScopeOrEmpty(ctx)); err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: stamp scope: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL statement_timeout = '%dms'`, RawQueryTimeout.Milliseconds())); err != nil {
		return nil, nil, false, fmt.Errorf("fabriq: set timeout: %w", err)
	}

	rows, qerr := tx.Query(ctx, sql, args...)
	if qerr != nil {
		return nil, nil, false, classifyQueryErr(qerr)
	}
	out, cols, truncated, serr := scanMapsCapped(rows, maxRawQueryRows)
	if serr != nil {
		return nil, nil, false, classifyQueryErr(serr)
	}
	return out, cols, truncated, nil
}

// assignMapDest writes a single map into the destination, which must be
// either *map[string]any or **map[string]any (or any pointer thereto).
func assignMapDest(into any, m map[string]any) error {
	switch dst := into.(type) {
	case *map[string]any:
		*dst = m
		return nil
	case **map[string]any:
		*dst = &m
		return nil
	default:
		return fmt.Errorf("fabriq: dynamic Get: destination must be *map[string]any, got %T", into)
	}
}

// assignMapsDest writes a slice of maps into the destination, which must be
// *[]map[string]any.
func assignMapsDest(into any, maps []map[string]any) error {
	switch dst := into.(type) {
	case *[]map[string]any:
		*dst = maps
		return nil
	default:
		return fmt.Errorf("fabriq: dynamic read: destination must be *[]map[string]any, got %T", into)
	}
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

// buildOrderExpr parses a comma-separated list of "col [ASC|DESC]" terms,
// validates each column against the entity binding (SQL-injection guard), and
// returns a fully-formed ORDER BY expression ready for interpolation into a
// query string. An empty orderBy returns "".
func buildOrderExpr(ent *registry.Entity, orderBy string) (string, error) {
	if orderBy == "" {
		return "", nil
	}
	terms := strings.Split(orderBy, ",")
	exprs := make([]string, 0, len(terms))
	for _, term := range terms {
		parts := strings.Fields(strings.TrimSpace(term))
		if len(parts) == 0 {
			continue
		}
		col := parts[0]
		dir := "ASC"
		if len(parts) > 1 {
			switch strings.ToUpper(parts[1]) {
			case "ASC", "DESC":
				dir = strings.ToUpper(parts[1])
			default:
				return "", fmt.Errorf("fabriq: invalid order direction %q", parts[1])
			}
		}
		if len(parts) > 2 || !ent.Binding.HasColumn(col) {
			return "", fmt.Errorf("fabriq: invalid order column %q for entity %q", term, ent.Spec.Name)
		}
		exprs = append(exprs, quoteIdent(col)+" "+dir)
	}
	return strings.Join(exprs, ", "), nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, ``) + `"`
}

func isNoRows(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows")
}
