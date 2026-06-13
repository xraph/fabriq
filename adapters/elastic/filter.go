package elastic

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// esFilterClauses translates an engine-neutral Cond list into Elasticsearch
// filter-context clauses (non-scoring). The query.SearchQuery validator has
// already checked columns and operators, so a residual unknown operator is
// an internal error.
//
// Exact-match clauses (term/terms/wildcard) target the `<field>.keyword`
// sub-field: the indexer relies on ES default dynamic mapping, which maps
// strings to `text` (analyzed, for multi_match) with a `keyword` sub-field
// (exact). Numeric values match the field directly.
func esFilterClauses(conds []query.Cond) ([]any, error) {
	out := make([]any, 0, len(conds))
	for _, c := range conds {
		clause, err := esFilterClause(c)
		if err != nil {
			return nil, err
		}
		out = append(out, clause)
	}
	return out, nil
}

func esFilterClause(c query.Cond) (map[string]any, error) {
	if c.IsGroup() {
		shoulds, err := esFilterClauses(c.Or)
		if err != nil {
			return nil, err
		}
		return map[string]any{"bool": map[string]any{
			"should":               shoulds,
			"minimum_should_match": 1,
		}}, nil
	}
	switch c.Op {
	case query.OpEq:
		return esTerm(c.Column, c.Value), nil
	case query.OpNe:
		return esMustNot(esTerm(c.Column, c.Value)), nil
	case query.OpIn:
		return esTerms(c.Column, c.Value), nil
	case query.OpNotIn:
		return esMustNot(esTerms(c.Column, c.Value)), nil
	case query.OpGt:
		return esRange(c.Column, "gt", c.Value), nil
	case query.OpGte:
		return esRange(c.Column, "gte", c.Value), nil
	case query.OpLt:
		return esRange(c.Column, "lt", c.Value), nil
	case query.OpLte:
		return esRange(c.Column, "lte", c.Value), nil
	case query.OpIsNull:
		return esMustNot(esExists(c.Column)), nil
	case query.OpIsNotNull:
		return esExists(c.Column), nil
	case query.OpLike:
		return esWildcard(c.Column, c.Value, false), nil
	case query.OpILike:
		return esWildcard(c.Column, c.Value, true), nil
	default:
		return nil, fmt.Errorf("fabriq: search filter cannot express operator %q on %q", c.Op, c.Column)
	}
}

func esTerm(col string, val any) map[string]any {
	return map[string]any{"term": map[string]any{esExactField(col, isStringValue(val)): val}}
}

func esTerms(col string, vals any) map[string]any {
	return map[string]any{"terms": map[string]any{esExactField(col, isStringSlice(vals)): vals}}
}

func esRange(col, op string, val any) map[string]any {
	return map[string]any{"range": map[string]any{col: map[string]any{op: val}}}
}

func esExists(col string) map[string]any {
	return map[string]any{"exists": map[string]any{"field": col}}
}

func esMustNot(clause map[string]any) map[string]any {
	return map[string]any{"bool": map[string]any{"must_not": clause}}
}

func esWildcard(col string, val any, caseInsensitive bool) map[string]any {
	pattern := ""
	if s, ok := val.(string); ok {
		pattern = sqlLikeToWildcard(s)
	}
	spec := map[string]any{"value": pattern}
	if caseInsensitive {
		spec["case_insensitive"] = true
	}
	return map[string]any{"wildcard": map[string]any{col + ".keyword": spec}}
}

// esExactField returns the sub-field to match exactly on: the keyword
// sub-field for strings, the field itself for numerics/bools.
func esExactField(col string, stringValued bool) string {
	if stringValued {
		return col + ".keyword"
	}
	return col
}

// esSortField returns the field to sort on: version (and any numeric) sorts
// directly; everything else sorts on its keyword sub-field (analyzed text
// is not sortable without fielddata).
func esSortField(col string) string {
	if col == registry.ColumnVersion {
		return col
	}
	return col + ".keyword"
}

func isStringValue(v any) bool {
	_, ok := v.(string)
	return ok
}

func isStringSlice(vals any) bool {
	rv := reflect.ValueOf(vals)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	if rv.Type().Elem().Kind() == reflect.String {
		return true
	}
	// []any of strings: inspect the first element.
	if rv.Len() > 0 && rv.Index(0).Kind() == reflect.Interface {
		_, ok := rv.Index(0).Interface().(string)
		return ok
	}
	return false
}

// sqlLikeToWildcard maps SQL LIKE wildcards to the ES wildcard grammar:
// % -> *, _ -> ?, with literal * and ? escaped.
func sqlLikeToWildcard(p string) string {
	var b strings.Builder
	for _, r := range p {
		switch r {
		case '%':
			b.WriteRune('*')
		case '_':
			b.WriteRune('?')
		case '*', '?':
			b.WriteRune('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
