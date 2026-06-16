// Package tenant carries the tenant identity on context.Context and is the
// single structural enforcement point for fabriq's tenancy invariant.
//
// Only auth middleware stamps tenants (from validated claims — never from
// forwarded headers). Every fabriq entry point calls Require and fails with
// ErrNoTenant on an unstamped context. Tenant IDs are validated at stamp
// time so that every name derived from them (graph names, index names,
// stream keys, cache prefixes) is safe by construction.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"regexp"
)

// ErrNoTenant is the canonical "unstamped context" error. The root fabriq
// package aliases it as fabriq.ErrNoTenant.
var ErrNoTenant = errors.New("fabriq: no tenant in context")

// ErrTenantHookTripped is returned by the relational backstop hook when a
// query reaches an engine without a tenant predicate. Aliased at root.
var ErrTenantHookTripped = errors.New("fabriq: tenant guard tripped")

type ctxKey struct{}

// idPattern keeps tenant IDs safe for every derived name: Redis stream keys
// (no ':'), ES indexes (no '/', no uppercase requirement enforced upstream),
// FalkorDB graph names, and SQL string settings.
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// Valid reports whether id is a well-formed tenant identifier.
func Valid(id string) bool { return idPattern.MatchString(id) }

// WithTenant returns a context stamped with the tenant id, or an error if
// the id is not a well-formed tenant identifier.
func WithTenant(ctx context.Context, id string) (context.Context, error) {
	if !Valid(id) {
		return nil, fmt.Errorf("fabriq: invalid tenant id %q (want %s)", id, idPattern)
	}
	return context.WithValue(ctx, ctxKey{}, id), nil
}

// MustWithTenant is WithTenant for wiring code with static ids; it panics on
// invalid input.
func MustWithTenant(ctx context.Context, id string) context.Context {
	out, err := WithTenant(ctx, id)
	if err != nil {
		panic(err)
	}
	return out
}

// FromContext returns the tenant stamped on ctx, or ErrNoTenant.
func FromContext(ctx context.Context) (string, error) {
	id, ok := ctx.Value(ctxKey{}).(string)
	if !ok || id == "" {
		return "", ErrNoTenant
	}
	return id, nil
}

// scopeKey is the context key for the optional secondary scope (a sub-tenant
// partition, e.g. a "project" within a "workspace"). Distinct from ctxKey so
// scope and tenant are independent.
type scopeKey struct{}

// WithScope returns a context stamped with an optional secondary scope, or an
// error if the scope is not a well-formed identifier. Scope is OPTIONAL: an
// unscoped context reads everything in the tenant; a scoped context reads its
// own scope plus shared (NULL-scope) rows.
func WithScope(ctx context.Context, scope string) (context.Context, error) {
	if !Valid(scope) {
		return nil, fmt.Errorf("fabriq: invalid scope id %q (want %s)", scope, idPattern)
	}
	return context.WithValue(ctx, scopeKey{}, scope), nil
}

// MustWithScope is WithScope for wiring code with static ids; it panics on invalid input.
func MustWithScope(ctx context.Context, scope string) context.Context {
	out, err := WithScope(ctx, scope)
	if err != nil {
		panic(err)
	}
	return out
}

// ScopeFromContext returns the scope stamped on ctx and ok=false when the
// context is unscoped. Unlike tenant, scope is optional — absence is legal.
func ScopeFromContext(ctx context.Context) (string, bool) {
	s, ok := ctx.Value(scopeKey{}).(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

// ScopeOrEmpty returns the scope stamped on ctx, or "" when unscoped. Use this
// for SET LOCAL app.scope_id and SQL arguments (empty = "see all in tenant").
func ScopeOrEmpty(ctx context.Context) string {
	s, _ := ScopeFromContext(ctx)
	return s
}
