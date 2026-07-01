package adminapi

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/xraph/fabriq/core/registry"
)

// --- wire types ---

type schemaWriteColumn struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	Default  string `json:"default,omitempty"`
}

type schemaWriteIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique,omitempty"`
}

type defineSchemaRequest struct {
	Type    string              `json:"type"`
	Columns []schemaWriteColumn `json:"columns"`
	Indexes []schemaWriteIndex  `json:"indexes,omitempty"`
}

type addFieldsRequest struct {
	Columns []schemaWriteColumn `json:"columns"`
	Indexes []schemaWriteIndex  `json:"indexes,omitempty"`
}

type renameFieldRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// --- helpers ---

var schemaIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validSchemaIdent(s string) bool { return schemaIdentRe.MatchString(s) }

func tableFor(typeName string) string { return "ds_" + typeName }

func kindToColumnType(kind string) (registry.ColumnType, error) {
	switch kind {
	case "string":
		return registry.ColText, nil
	case "number":
		return registry.ColFloat, nil
	case "boolean":
		return registry.ColBool, nil
	case "time":
		return registry.ColTime, nil
	case "object":
		return registry.ColJSON, nil
	default:
		return 0, fmt.Errorf("unknown column kind %q (want string|number|boolean|time|object)", kind)
	}
}

var (
	defNumRe = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
	defStrRe = regexp.MustCompile(`^'[^']*'$`)
)

// validateDefaultExpr allows only a strict allowlist of SQL default expressions,
// because DynamicColumn.Default is interpolated verbatim into DDL.
func validateDefaultExpr(s string) error {
	if s == "" {
		return nil
	}
	switch strings.ToLower(s) {
	case "true", "false", "null", "now()":
		return nil
	}
	if defNumRe.MatchString(s) || defStrRe.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid column default %q: allowed forms are a number, true/false/null, now(), or a single-quoted string literal", s)
}

// columnsToRegistry translates wire columns to registry.DynamicColumn, validating
// each name/kind/default. Returns a 400-worthy error on the first problem.
func columnsToRegistry(cols []schemaWriteColumn) ([]registry.DynamicColumn, error) {
	out := make([]registry.DynamicColumn, 0, len(cols))
	for _, c := range cols {
		if !validSchemaIdent(c.Name) {
			return nil, fmt.Errorf("invalid column name %q", c.Name)
		}
		ct, err := kindToColumnType(c.Kind)
		if err != nil {
			return nil, err
		}
		if err := validateDefaultExpr(c.Default); err != nil {
			return nil, err
		}
		out = append(out, registry.DynamicColumn{Name: c.Name, Type: ct, NotNull: c.Required, Default: c.Default})
	}
	return out, nil
}

func indexesToRegistry(idx []schemaWriteIndex) ([]registry.DynamicIndex, error) {
	out := make([]registry.DynamicIndex, 0, len(idx))
	for _, i := range idx {
		if !validSchemaIdent(i.Name) {
			return nil, fmt.Errorf("invalid index name %q", i.Name)
		}
		for _, c := range i.Columns {
			if !validSchemaIdent(c) {
				return nil, fmt.Errorf("invalid index column %q", c)
			}
		}
		out = append(out, registry.DynamicIndex{Name: i.Name, Columns: i.Columns, Unique: i.Unique})
	}
	return out, nil
}
