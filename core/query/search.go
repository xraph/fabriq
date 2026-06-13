package query

import (
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/registry"
)

// SortField splits a "column [DESC]" sort spec into the column and whether
// it is descending. Empty input yields an empty column — sort by relevance.
// Adapters and the validator share it so they agree on how a sort is read.
func SortField(sort string) (column string, desc bool) {
	parts := strings.Fields(sort)
	if len(parts) == 0 {
		return "", false
	}
	return parts[0], len(parts) > 1 && strings.EqualFold(parts[1], "DESC")
}

// searchIndexed reports whether a column lives in an entity's search index:
// its declared search fields plus the structural id/tenant_id/version that
// every indexed document carries. You can only filter or sort on what the
// index actually holds.
func searchIndexed(searchFields []string) func(string) bool {
	allowed := map[string]struct{}{
		registry.ColumnID:      {},
		registry.ColumnTenant:  {},
		registry.ColumnVersion: {},
	}
	for _, f := range searchFields {
		allowed[f] = struct{}{}
	}
	return func(c string) bool {
		_, ok := allowed[c]
		return ok
	}
}

// ValidateSearchQuery checks a SearchQuery's Filter and Sort against the
// entity's indexed fields and the operator vocabulary — so every search
// adapter rejects the same unknown-column / bad-operator inputs (and the
// same injection surface) before translating to its engine. searchFields
// is the entity's declared search fields; id/tenant_id/version are always
// allowed.
func ValidateSearchQuery(q SearchQuery, searchFields []string) error {
	has := searchIndexed(searchFields)
	if err := ValidateConds(q.Filter, has); err != nil {
		return err
	}
	if q.Sort != "" {
		col, _ := SortField(q.Sort)
		if !has(col) {
			return fmt.Errorf("fabriq: search sort references unknown indexed column %q", col)
		}
	}
	return nil
}
