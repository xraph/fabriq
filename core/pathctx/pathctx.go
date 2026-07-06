// Package pathctx carries the tenant's Postgres schema on context.Context
// for schema-per-tenant "consolidation mode": the adapter stamps
// SET LOCAL search_path from it per transaction, exactly as core/tenant's
// id is stamped into SET LOCAL app.tenant_id. The value is OPTIONAL — when
// absent (single/shards/database modes) the adapter stamps nothing new and
// behaves identically.
package pathctx

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// schemaPattern keeps a stamped schema a safe, bare, lowercase Postgres
// identifier (no quoting, no injection): the "tenant_" prefix plus the
// mapped tenant id. 54 trailing chars keeps the whole name under
// NAMEDATALEN (63).
var schemaPattern = regexp.MustCompile(`^tenant_[a-z0-9_]{1,54}$`)

// ValidSchema reports whether s is a well-formed tenant schema name.
func ValidSchema(s string) bool { return schemaPattern.MatchString(s) }

// SchemaForTenant maps a tenant id to its consolidation-mode schema name:
// "tenant_" + the id with '-' folded to '_' (tenant ids allow '-', which is
// illegal in a bare Postgres identifier). It fails CLOSED for ids that
// cannot map to a legal schema — uppercase (Postgres folds unquoted
// identifiers to lowercase, so "Acme" and "acme" would collide) or too long
// — rather than silently folding to a colliding name. The mapping is
// deterministic, so routing derives the schema without a catalog lookup;
// provisioning persists the same value in Entry.Schema.
func SchemaForTenant(tenantID string) (string, error) {
	s := "tenant_" + strings.ReplaceAll(tenantID, "-", "_")
	if !ValidSchema(s) {
		return "", fmt.Errorf("fabriq: tenant %q has no valid consolidation schema (need lowercase, <=55 chars after '-'→'_')", tenantID)
	}
	return s, nil
}

type ctxKey struct{}

// WithSearchPath stamps the tenant schema on ctx, or errors if malformed.
func WithSearchPath(ctx context.Context, schema string) (context.Context, error) {
	if !ValidSchema(schema) {
		return nil, fmt.Errorf("fabriq: invalid tenant schema %q (want %s)", schema, schemaPattern)
	}
	return context.WithValue(ctx, ctxKey{}, schema), nil
}

// MustWithSearchPath is WithSearchPath for wiring code; panics on invalid.
func MustWithSearchPath(ctx context.Context, schema string) context.Context {
	out, err := WithSearchPath(ctx, schema)
	if err != nil {
		panic(err)
	}
	return out
}

// SchemaOrEmpty returns the stamped schema, or "" when none is set.
func SchemaOrEmpty(ctx context.Context) string {
	s, _ := ctx.Value(ctxKey{}).(string)
	return s
}
