package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/xraph/fabriq/core/registry"
)

// ddlIdent validates identifiers interpolated into DDL text.
// Identifiers (table, column, index names) are re-validated here with
// ddlValid at the SQL boundary, in addition to the registry's checks.
// Note: DynamicColumn.Default is an SQL expression, not an identifier — it
// is interpolated verbatim and must be control-plane-trusted (see its doc).
var ddlIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func ddlValid(s string) bool { return ddlIdent.MatchString(s) }

// sqlType maps a registry ColumnType to the Postgres type name.
func sqlType(t registry.ColumnType) string {
	switch t {
	case registry.ColText:
		return "TEXT"
	case registry.ColInt:
		return "BIGINT"
	case registry.ColFloat:
		return "DOUBLE PRECISION"
	case registry.ColBool:
		return "BOOLEAN"
	case registry.ColTime:
		return "TIMESTAMPTZ"
	case registry.ColJSON:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// execDDL runs a single DDL statement via the pg driver's pool path.
// DDL is always schema-owner work — it must NOT run inside a tenant
// transaction (which is RLS-constrained to the app role).
func (a *Adapter) execDDL(ctx context.Context, stmt string) error {
	if _, err := a.pg.Exec(ctx, stmt); err != nil {
		// Classify at the source so every DDL caller gets a structured error
		// whose caller-facing message is driver-free. The raw statement is
		// deliberately NOT attached — it would leak internal SQL to clients;
		// the driver cause remains reachable via Unwrap for server logs.
		return translatePg("ddl", "", "", err)
	}
	return nil
}

// EnsureDynamic creates or ADDITIVELY EVOLVES the Postgres table for a
// dynamic entity from its descriptor: structural columns (id, tenant_id,
// version), declared domain columns, a tenant index, any descriptor-declared
// secondary indexes, and tenant-isolation RLS — mirroring the patterns in
// migrations/0003_site_asset_tag.go and migrations/0004_rls_policies.go.
//
// # Additive-evolution policy
//
// When called on an already-existing table (e.g. because the consumer changed
// the descriptor by adding columns or indexes), EnsureDynamic reconciles the
// schema ADDITIVELY:
//
//   - Each domain column is emitted as "ALTER TABLE … ADD COLUMN IF NOT
//     EXISTS …", so new columns appear and existing ones are left untouched.
//   - Each index is emitted as "CREATE INDEX IF NOT EXISTS …", so new indexes
//     are created and existing ones are left untouched.
//
// DROPS, RENAMES, and TYPE CHANGES are NOT auto-applied. If a column or
// index is removed from the descriptor, the physical column/index remains —
// evolution is strictly additive. Consumers that need destructive changes must
// write an explicit migration.
//
// Attempting to add a NOT-NULL column (without a DEFAULT) to a table that
// already has rows will fail at the Postgres level. This is intentional: the
// consumer must either supply a Default expression in the descriptor or add
// the column as nullable. fabriq does not work around this safety boundary.
//
// This is the FENCED managed-DDL lane: fabriq manages DDL ONLY for dynamic
// entities. Static entities keep migrations as the authority; calling
// EnsureDynamic for a non-dynamic entity (Spec.Schema == nil) is an error.
//
// Run as the schema owner (superuser DSN), not the RLS-constrained app role.
// It is safe to run from several processes at once and on every boot: the
// whole ensure runs in one transaction under a per-table advisory lock, and
// the RLS step only adds what is missing (see ensureDynamicTx).
//
// The entity must be registered in the registry BEFORE the adapter is
// constructed (fabriq.Open), so the pool-path tenant backstop includes the
// dynamic table in its guarded set; EnsureDynamic only creates the physical
// table, it does not register the entity.
func (a *Adapter) EnsureDynamic(ctx context.Context, ent *registry.Entity) error {
	s := ent.Spec.Schema
	if s == nil {
		return fmt.Errorf("fabriq: EnsureDynamic called for non-dynamic entity %q", ent.Spec.Name)
	}
	if !ddlValid(s.Table) {
		return fmt.Errorf("fabriq: invalid dynamic table name %q", s.Table)
	}

	// Validate all domain column names up-front before building any SQL.
	for _, c := range s.Columns {
		if !ddlValid(c.Name) {
			return fmt.Errorf("fabriq: invalid dynamic column name %q in table %q", c.Name, s.Table)
		}
	}

	// Build column definitions. Structural columns are injected first;
	// the descriptor lists only domain columns.
	cols := []string{
		`id TEXT PRIMARY KEY`,
		`tenant_id TEXT NOT NULL`,
		`version BIGINT NOT NULL`,
	}
	for _, c := range s.Columns {
		cols = append(cols, domainColumnDef(c))
	}

	stmts := []string{
		// Table — idempotent via IF NOT EXISTS.
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", s.Table, strings.Join(cols, ",\n")),
		// Mandatory tenant index (mirrors the static migration pattern).
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_tenant_idx ON %s (tenant_id)", s.Table, s.Table),
	}

	// Additive column evolution: emit ADD COLUMN IF NOT EXISTS for each domain
	// column. On a fresh table all of these are no-ops (the columns already
	// exist from the CREATE above). On an existing table only truly new columns
	// are added; existing columns — even if their definition in the descriptor
	// has changed — are left untouched (see policy in the doc comment).
	for _, c := range s.Columns {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s",
			s.Table, domainColumnDef(c),
		))
	}

	// Descriptor-declared secondary indexes.
	for _, idx := range s.Indexes {
		if !ddlValid(idx.Name) {
			return fmt.Errorf("fabriq: invalid index name %q on table %q", idx.Name, s.Table)
		}
		for _, col := range idx.Columns {
			if !ddlValid(col) {
				return fmt.Errorf("fabriq: invalid index column %q in index %q on table %q", col, idx.Name, s.Table)
			}
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		stmts = append(stmts, fmt.Sprintf(
			"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
			unique, idx.Name, s.Table, strings.Join(idx.Columns, ", "),
		))
	}

	return a.ensureDynamicTx(ctx, s.Table, stmts)
}

// dynamicDDLLockSeed seeds hashtextextended for the per-table advisory lock
// EnsureDynamic holds. The hashed text carries a "fabriq_dynamic_ddl:" prefix
// too, so the key cannot meet the document plane's per-doc locks.
const dynamicDDLLockSeed = 11

// tenantPolicySQL is the tenant-isolation policy every dynamic table gets,
// mirroring migrations/0004_rls_policies.go exactly.
const tenantPolicySQL = `CREATE POLICY tenant_isolation ON %s USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`

// ensureDynamicTx runs the table, column and index statements for one dynamic
// table, then its RLS step, in a single transaction that holds a
// transaction-scoped advisory lock keyed on the table name. Every boot, sync
// pass and reconcile re-runs this, often from several processes at once, so:
//
//   - the lock serialises concurrent ensures of the same table (two
//     CREATE TABLE IF NOT EXISTS or CREATE POLICY racing each other fail with
//     a duplicate error otherwise);
//   - ENABLE and FORCE ROW LEVEL SECURITY run only when pg_class says they
//     are off;
//   - the policy is created only when pg_policy has none by that name on the
//     table. It is never dropped and re-created, so an RLS-bound reader never
//     meets a forced table with no policy (which reads as zero rows).
//
// Name presence is enough for the policy: its definition has not changed
// since the dynamic lane shipped. If it ever does, compare pg_policies.qual
// and with_check here and replace the policy inside this same transaction.
func (a *Adapter) ensureDynamicTx(ctx context.Context, table string, stmts []string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("fabriq: ensure dynamic table %s: %w", table, translatePg("ddl", "", "", err))
		}
	}()
	tx, err := a.pg.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`,
		"fabriq_dynamic_ddl:"+table, dynamicDDLLockSeed); err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err = tx.Exec(ctx, stmt); err != nil {
			return err
		}
	}

	var enabled, forced, hasPolicy bool
	if qerr := tx.QueryRow(ctx, `SELECT c.relrowsecurity, c.relforcerowsecurity,
		EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid AND p.polname = 'tenant_isolation')
		FROM pg_class c WHERE c.oid = to_regclass($1)`, table).Scan(&enabled, &forced, &hasPolicy); qerr != nil {
		return qerr
	}
	var rls []string
	if !enabled {
		rls = append(rls, fmt.Sprintf(`ALTER TABLE %s ENABLE ROW LEVEL SECURITY`, table))
	}
	if !forced {
		rls = append(rls, fmt.Sprintf(`ALTER TABLE %s FORCE ROW LEVEL SECURITY`, table))
	}
	if !hasPolicy {
		rls = append(rls, fmt.Sprintf(tenantPolicySQL, table))
	}
	for _, stmt := range rls {
		if _, err = tx.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// assertMutableColumn rejects structural columns (id, tenant_id, version) and
// invalid identifiers — the shared guard for destructive column DDL.
func assertMutableColumn(col string) error {
	switch col {
	case registry.ColumnID, registry.ColumnTenant, registry.ColumnVersion:
		return fmt.Errorf("fabriq: column %q is structural and cannot be dropped or renamed", col)
	}
	if !ddlValid(col) {
		return fmt.Errorf("fabriq: invalid column name %q", col)
	}
	return nil
}

// DropDynamicColumn drops a domain column from a dynamic table. Structural
// columns are refused. Runs as schema owner. Idempotent (IF EXISTS).
func (a *Adapter) DropDynamicColumn(ctx context.Context, table, column string) error {
	if !ddlValid(table) {
		return fmt.Errorf("fabriq: invalid dynamic table name %q", table)
	}
	if err := assertMutableColumn(column); err != nil {
		return err
	}
	stmt := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
		quoteIdent(table), quoteIdent(column))
	return a.execDDL(ctx, stmt)
}

// RenameDynamicColumn renames a domain column on a dynamic table. The source
// column must be non-structural and valid; the target must be a valid,
// non-structural identifier.
func (a *Adapter) RenameDynamicColumn(ctx context.Context, table, oldName, newName string) error {
	if !ddlValid(table) {
		return fmt.Errorf("fabriq: invalid dynamic table name %q", table)
	}
	if err := assertMutableColumn(oldName); err != nil {
		return err
	}
	if err := assertMutableColumn(newName); err != nil {
		return err
	}
	stmt := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		quoteIdent(table), quoteIdent(oldName), quoteIdent(newName))
	return a.execDDL(ctx, stmt)
}

// DropDynamic drops a dynamic entity's table (its indexes and RLS policies drop
// with it). Idempotent (IF EXISTS). Destructive — callers gate confirmation.
func (a *Adapter) DropDynamic(ctx context.Context, table string) error {
	if !ddlValid(table) {
		return fmt.Errorf("fabriq: invalid dynamic table name %q", table)
	}
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(table))
	return a.execDDL(ctx, stmt)
}

// domainColumnDef returns the SQL column-definition fragment for a domain
// column: "<name> <type> [NOT NULL] [DEFAULT <expr>]".
//
// This is the single source of truth shared by both the CREATE TABLE column
// list and the ALTER TABLE ADD COLUMN statements, so the two can never drift.
// The Default expression is interpolated verbatim and must be a trusted,
// control-plane value (see DynamicColumn.Default for the contract).
func domainColumnDef(c registry.DynamicColumn) string {
	def := fmt.Sprintf("%s %s", c.Name, sqlType(c.Type))
	if c.NotNull {
		def += " NOT NULL"
	}
	if c.Default != "" {
		def += " DEFAULT " + c.Default
	}
	return def
}
