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
		return err
	}
	return nil
}

// EnsureDynamic creates (idempotently) the Postgres table for a dynamic
// entity from its descriptor: structural columns (id, tenant_id, version),
// declared domain columns, a tenant index, any descriptor-declared
// secondary indexes, and tenant-isolation RLS — mirroring the patterns in
// migrations/0003_site_asset_tag.go and migrations/0004_rls_policies.go.
//
// This is the FENCED managed-DDL lane: fabriq manages DDL ONLY for dynamic
// entities. Static entities keep migrations as the authority; calling
// EnsureDynamic for a non-dynamic entity (Spec.Schema == nil) is an error.
//
// Run as the schema owner (superuser DSN), not the RLS-constrained app role.
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

	// Build column definitions. Structural columns are injected first;
	// the descriptor lists only domain columns.
	cols := []string{
		`id TEXT PRIMARY KEY`,
		`tenant_id TEXT NOT NULL`,
		`version BIGINT NOT NULL`,
	}
	for _, c := range s.Columns {
		if !ddlValid(c.Name) {
			return fmt.Errorf("fabriq: invalid dynamic column name %q in table %q", c.Name, s.Table)
		}
		def := fmt.Sprintf("%s %s", c.Name, sqlType(c.Type))
		if c.NotNull {
			def += " NOT NULL"
		}
		if c.Default != "" {
			// The Default literal is descriptor-controlled; it is the
			// consumer's responsibility to supply a valid SQL expression.
			def += " DEFAULT " + c.Default
		}
		cols = append(cols, def)
	}

	stmts := []string{
		// Table — idempotent via IF NOT EXISTS.
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", s.Table, strings.Join(cols, ",\n")),
		// Mandatory tenant index (mirrors the static migration pattern).
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_tenant_idx ON %s (tenant_id)", s.Table, s.Table),
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

	// Tenant-isolation RLS — mirrors migrations/0004_rls_policies.go exactly.
	stmts = append(stmts,
		fmt.Sprintf(`ALTER TABLE %s ENABLE ROW LEVEL SECURITY`, s.Table),
		fmt.Sprintf(`ALTER TABLE %s FORCE ROW LEVEL SECURITY`, s.Table),
		fmt.Sprintf(`DROP POLICY IF EXISTS tenant_isolation ON %s`, s.Table),
		fmt.Sprintf(`CREATE POLICY tenant_isolation ON %s USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`, s.Table),
	)

	for _, stmt := range stmts {
		if err := a.execDDL(ctx, stmt); err != nil {
			return fmt.Errorf("fabriq: ensure dynamic table %s: %w\nstatement: %s", s.Table, err, stmt)
		}
	}
	return nil
}
