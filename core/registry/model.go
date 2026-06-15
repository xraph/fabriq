package registry

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/xraph/grove/schema"
)

// dynIdent is the identifier pattern accepted for dynamic table and column names.
var dynIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// Structural columns every fabriq-managed table must carry. They are what
// make the fabric invariants enforceable: tenancy is a column (RLS), and
// optimistic concurrency is a column (version).
const (
	ColumnID      = "id"
	ColumnTenant  = "tenant_id"
	ColumnVersion = "version"
)

// Binding is the compiled relational shape of an entity, derived from its
// grove-tagged model at registration time, or from a DynamicSchema.
type Binding struct {
	Table         string
	Columns       []string
	PK            string
	TenantColumn  string
	VersionColumn string

	modelType reflect.Type
	fields    map[string]*schema.Field

	// dynamic is true when the binding was built from a DynamicSchema rather
	// than a Go struct model.
	dynamic bool
	dynCols map[string]DynamicColumn
}

// HasColumn reports whether the bound table has the given column.
func (b *Binding) HasColumn(col string) bool {
	if b.dynamic {
		_, ok := b.dynCols[col]
		return ok
	}
	_, ok := b.fields[col]
	return ok
}

// Required returns the non-structural columns that must be provided on
// create/update: NOT NULL, no default, not auto-generated.
func (b *Binding) Required() []string {
	if b.dynamic {
		var out []string
		for _, col := range b.Columns {
			if col == ColumnID || col == ColumnTenant || col == ColumnVersion {
				continue
			}
			dc := b.dynCols[col]
			if dc.NotNull && dc.Default == "" {
				out = append(out, col)
			}
		}
		return out
	}
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
func (b *Binding) NewModel() any {
	if b.dynamic {
		panic(fmt.Sprintf("fabriq: NewModel is not supported for dynamic entity %s (dynamic entities use map-native values)", b.Table))
	}
	return reflect.New(b.modelType).Interface()
}

// ValuesByColumn extracts the model's field values keyed by column name.
// This is the canonical payload shape: event payloads, graph node props and
// search documents are all column-keyed.
// For dynamic entities model must be a map[string]any; unknown keys are dropped.
func (b *Binding) ValuesByColumn(model any) (map[string]any, error) {
	if b.dynamic {
		if model == nil {
			return nil, fmt.Errorf("fabriq: dynamic entity %s: nil payload", b.Table)
		}
		m, ok := model.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("fabriq: dynamic entity %s expects a map[string]any payload, got %T", b.Table, model)
		}
		if m == nil {
			return nil, fmt.Errorf("fabriq: dynamic entity %s: nil payload", b.Table)
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			if _, known := b.dynCols[k]; known {
				out[k] = v
			}
		}
		return out, nil
	}
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

// Populate sets the model's fields from column-keyed values (the reverse
// of ValuesByColumn). Numeric JSON widening (float64 -> int64 etc.) is
// converted; incompatible types error.
func (b *Binding) Populate(model any, vals map[string]any) error {
	if b.dynamic {
		return fmt.Errorf("fabriq: Populate is not supported for dynamic entity %s (dynamic entities use map-native values)", b.Table)
	}
	v := reflect.ValueOf(model)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("fabriq: Populate target must be a non-nil pointer, got %T", model)
	}
	v = v.Elem()
	if v.Type() != b.modelType {
		return fmt.Errorf("fabriq: Populate target %s does not match binding for table %s (want %s)",
			v.Type(), b.Table, b.modelType)
	}
	for col, f := range b.fields {
		raw, ok := vals[col]
		if !ok || raw == nil {
			continue
		}
		fv := v.FieldByIndex(f.GoIndex)
		rv := reflect.ValueOf(raw)
		switch {
		case rv.Type() == fv.Type():
			fv.Set(rv)
		case rv.Type().ConvertibleTo(fv.Type()):
			fv.Set(rv.Convert(fv.Type()))
		default:
			return fmt.Errorf("fabriq: column %q value %T not assignable to field %s %s",
				col, raw, f.GoName, fv.Type())
		}
	}
	return nil
}

// bind compiles the spec into a Binding. For dynamic entities it routes to
// bindDynamic; for Go-model entities it uses reflection via grove/schema.
func bind(spec EntitySpec) (*Binding, error) {
	if spec.Schema != nil {
		if spec.Model != nil {
			return nil, fmt.Errorf("fabriq: entity %q: Model and Schema are mutually exclusive", spec.Name)
		}
		return bindDynamic(spec)
	}
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

// bindDynamic builds a Binding from a DynamicSchema without reflection.
// Structural columns (id, tenant_id, version) are injected automatically;
// callers must not redeclare them.
func bindDynamic(spec EntitySpec) (*Binding, error) {
	s := spec.Schema
	if s.Table == "" || !dynIdent.MatchString(s.Table) {
		return nil, fmt.Errorf("fabriq: entity %q: dynamic schema needs a valid Table identifier, got %q", spec.Name, s.Table)
	}
	b := &Binding{
		Table:   s.Table,
		dynamic: true,
		dynCols: make(map[string]DynamicColumn),
	}

	add := func(c DynamicColumn) error {
		if _, dup := b.dynCols[c.Name]; dup {
			return fmt.Errorf("fabriq: entity %q: duplicate column %q", spec.Name, c.Name)
		}
		b.dynCols[c.Name] = c
		b.Columns = append(b.Columns, c.Name)
		return nil
	}

	// Inject structural columns first.
	for _, c := range []DynamicColumn{
		{Name: ColumnID, Type: ColText, NotNull: true},
		{Name: ColumnTenant, Type: ColText, NotNull: true},
		{Name: ColumnVersion, Type: ColInt, NotNull: true},
	} {
		_ = add(c) // structural names are distinct; cannot fail
	}

	// Zero domain columns is valid (e.g. a junction/lookup table): the entity
	// still carries the injected structural columns.
	// Register domain columns.
	for _, c := range s.Columns {
		if c.Name == ColumnID || c.Name == ColumnTenant || c.Name == ColumnVersion {
			return nil, fmt.Errorf("fabriq: entity %q: column %q is structural and injected automatically", spec.Name, c.Name)
		}
		if !dynIdent.MatchString(c.Name) {
			return nil, fmt.Errorf("fabriq: entity %q: invalid column identifier %q", spec.Name, c.Name)
		}
		if err := add(c); err != nil {
			return nil, err
		}
	}

	// Validate index column references.
	for _, idx := range s.Indexes {
		for _, col := range idx.Columns {
			if _, ok := b.dynCols[col]; !ok {
				return nil, fmt.Errorf("fabriq: entity %q: index %q references unknown column %q", spec.Name, idx.Name, col)
			}
		}
	}

	b.PK, b.TenantColumn, b.VersionColumn = ColumnID, ColumnTenant, ColumnVersion
	return b, nil
}
