package postgres

import (
	"context"
	"fmt"
)

// ColumnInfo describes one physical column of a table, read from
// information_schema. Used by the admin schema-drift diagnostics.
type ColumnInfo struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
	Nullable bool   `json:"nullable"`
}

// TableColumns returns the physical columns of a public-schema table from
// information_schema, in ordinal order. A non-existent table yields an empty
// slice (no error). Read-only; information_schema is not tenant-scoped, so this
// runs on the pool without a tenant transaction.
func (a *Adapter) TableColumns(ctx context.Context, table string) ([]ColumnInfo, error) {
	if !ddlValid(table) {
		return nil, fmt.Errorf("fabriq: invalid table name %q", table)
	}
	const q = `SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`
	rows, err := a.pg.Query(ctx, q, table)
	if err != nil {
		return nil, fmt.Errorf("fabriq: TableColumns query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ColumnInfo
	for rows.Next() {
		var name, dtype, nullable string
		if err := rows.Scan(&name, &dtype, &nullable); err != nil {
			return nil, fmt.Errorf("fabriq: TableColumns scan: %w", err)
		}
		out = append(out, ColumnInfo{Name: name, DataType: dtype, Nullable: nullable == "YES"})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fabriq: TableColumns rows.Err: %w", err)
	}
	return out, nil
}
