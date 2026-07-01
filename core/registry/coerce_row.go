package registry

import "fmt"

// CoerceRow coerces and type-checks a dynamic entity's column-keyed values in
// place against their declared ColumnTypes, replacing each value with its
// canonical Go type. It is a no-op for Go-model entities (their struct fields
// are already typed) and skips the structural columns id/tenant_id/version,
// which are stamped by the executor / materializer rather than typed by the
// caller. A returned error names the offending entity and column.
func CoerceRow(ent *Entity, vals map[string]any) error {
	if !ent.Binding.IsDynamic() {
		return nil
	}
	for col, v := range vals {
		if col == ColumnID || col == ColumnTenant || col == ColumnVersion {
			continue
		}
		dc, ok := ent.Binding.DynColumn(col)
		if !ok {
			continue
		}
		coerced, err := CoerceToColumn(dc.Type, v)
		if err != nil {
			return fmt.Errorf("fabriq: entity %q: column %q %w", ent.Spec.Name, col, err)
		}
		vals[col] = coerced
	}
	return nil
}
