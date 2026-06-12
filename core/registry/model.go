package registry

import (
	"fmt"
	"reflect"

	"github.com/xraph/grove/schema"
)

// Structural columns every fabriq-managed table must carry. They are what
// make the fabric invariants enforceable: tenancy is a column (RLS), and
// optimistic concurrency is a column (version).
const (
	ColumnID      = "id"
	ColumnTenant  = "tenant_id"
	ColumnVersion = "version"
)

// Binding is the compiled relational shape of an entity, derived from its
// grove-tagged model at registration time.
type Binding struct {
	Table         string
	Columns       []string
	PK            string
	TenantColumn  string
	VersionColumn string

	modelType reflect.Type
	fields    map[string]*schema.Field
}

// HasColumn reports whether the bound table has the given column.
func (b *Binding) HasColumn(col string) bool {
	_, ok := b.fields[col]
	return ok
}

// Required returns the non-structural columns that must be provided on
// create/update: NOT NULL, no default, not auto-generated.
func (b *Binding) Required() []string {
	var out []string
	for _, col := range b.Columns {
		if col == ColumnID || col == ColumnTenant || col == ColumnVersion {
			continue
		}
		f := b.fields[col]
		if f.Options.NotNull && f.Options.Default == "" && !f.Options.AutoIncrement {
			out = append(out, col)
		}
	}
	return out
}

// ModelType returns the bound struct type (not pointer).
func (b *Binding) ModelType() reflect.Type { return b.modelType }

// NewModel returns a pointer to a fresh zero value of the bound model type.
func (b *Binding) NewModel() any { return reflect.New(b.modelType).Interface() }

// ValuesByColumn extracts the model's field values keyed by column name.
// This is the canonical payload shape: event payloads, graph node props and
// search documents are all column-keyed.
func (b *Binding) ValuesByColumn(model any) (map[string]any, error) {
	v := reflect.ValueOf(model)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("fabriq: nil model for table %s", b.Table)
		}
		v = v.Elem()
	}
	if v.Type() != b.modelType {
		return nil, fmt.Errorf("fabriq: model type %s does not match binding for table %s (want %s)",
			v.Type(), b.Table, b.modelType)
	}
	out := make(map[string]any, len(b.Columns))
	for col, f := range b.fields {
		out[col] = v.FieldByIndex(f.GoIndex).Interface()
	}
	return out, nil
}

// bind compiles the spec's grove model into a Binding and enforces the
// structural-column contract.
func bind(spec EntitySpec) (*Binding, error) {
	if spec.Model == nil {
		return nil, fmt.Errorf("fabriq: entity %q: model is required", spec.Name)
	}
	table, err := schema.NewTable(spec.Model)
	if err != nil {
		return nil, fmt.Errorf("fabriq: entity %q: %w", spec.Name, err)
	}
	if table.Name == "" {
		return nil, fmt.Errorf("fabriq: entity %q: model has no table name", spec.Name)
	}

	b := &Binding{
		Table:     table.Name,
		modelType: table.ModelType,
		fields:    make(map[string]*schema.Field, len(table.Fields)),
	}
	for _, f := range table.Fields {
		if f.Options.Skip || f.Options.Column == "" {
			continue
		}
		b.fields[f.Options.Column] = f
		b.Columns = append(b.Columns, f.Options.Column)
	}

	if f, ok := b.fields[ColumnID]; !ok || !f.Options.IsPK {
		return nil, fmt.Errorf("fabriq: entity %q: model must declare column %q as primary key", spec.Name, ColumnID)
	}
	b.PK = ColumnID
	if !b.HasColumn(ColumnTenant) {
		return nil, fmt.Errorf("fabriq: entity %q: model must declare column %q (tenancy is structural)", spec.Name, ColumnTenant)
	}
	b.TenantColumn = ColumnTenant
	if !b.HasColumn(ColumnVersion) {
		return nil, fmt.Errorf("fabriq: entity %q: model must declare column %q (every write is versioned)", spec.Name, ColumnVersion)
	}
	b.VersionColumn = ColumnVersion

	return b, nil
}
